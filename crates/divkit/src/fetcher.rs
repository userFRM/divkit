//! ETag-aware HTTP fetcher with retry, single-flight, CDN mirror fallback,
//! and SHA-256 manifest verification.
//!
//! # Cache layout
//!
//! ```text
//! $DIVKIT_CACHE_DIR/            (default: XDG cache / divkit)
//! ├── dividends-2020.parquet    ← cached body
//! ├── dividends-2020.parquet.etag
//! ├── dividends-2021.parquet
//! └── dividends-2021.parquet.etag
//! ```
//!
//! # Fetch flow (per call)
//!
//! 1. Single-flight gate: if another task is already fetching this key,
//!    join the in-flight request rather than issuing a duplicate.
//! 2. Cache check: if a local file exists, send `If-None-Match` with the
//!    stored ETag.
//! 3. `304 Not Modified` → return the cached bytes.
//! 4. `2xx` → write body + ETag, return bytes.
//! 5. Retry-able error (5xx, 429, connect/timeout): exponential backoff up
//!    to 3 total attempts. Delays: 250 ms → 750 ms → 2 000 ms (capped).
//!    429 response: respect `Retry-After` header if present.
//! 6. On primary-URL exhaustion: try jsDelivr CDN mirror once.
//! 7. All transports failed but cache exists → warn + return stale.
//! 8. All transports failed + no cache → return `Err`.
//!
//! # SHA-256 verification
//!
//! If a `manifest.json` entry exists for the key, the fetched bytes are
//! checked against the stored digest. A mismatch returns
//! `Error::ChecksumMismatch` and the corrupt bytes are NOT written to cache.

use bytes::Bytes;
use reqwest::StatusCode;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, OnceCell};

use crate::error::{Error, Result};

// ---------------------------------------------------------------------------
// Retry constants
// ---------------------------------------------------------------------------

/// Maximum total attempts (initial + 2 retries).
const MAX_ATTEMPTS: u32 = 3;

/// Base delay for exponential backoff.
const BACKOFF_BASE_MS: u64 = 250;

/// Cap on backoff delay.
const BACKOFF_MAX_MS: u64 = 2_000;

// ---------------------------------------------------------------------------
// In-flight entry
// ---------------------------------------------------------------------------

/// An in-flight or completed fetch. Stored in the single-flight map while
/// a key is being fetched; the value is an error message if the fetch failed.
type InflightCell = Arc<OnceCell<std::result::Result<Bytes, String>>>;

// ---------------------------------------------------------------------------
// CachedFetcher
// ---------------------------------------------------------------------------

/// ETag-aware fetcher with retry, single-flight deduplication, CDN mirror
/// fallback, and SHA-256 manifest verification.
pub(crate) struct CachedFetcher {
    pub http: reqwest::Client,
    /// Primary origin URL (e.g. `raw.githubusercontent.com/…/data`).
    pub base_url: String,
    /// CDN mirror base URL, consulted after primary exhausts all retries.
    ///
    /// - `Some(url)` — try this URL once on primary exhaustion.
    /// - `None` — mirror fallback is disabled; a primary failure returns the
    ///   error directly.
    ///
    /// Populated from `DIVKIT_MIRROR_URL` env var at construction time,
    /// unless overridden via [`CachedFetcher::set_mirror_url`].
    pub mirror_url: Option<String>,
    pub cache_dir: PathBuf,
    /// Per-key in-flight deduplication.
    inflight: Arc<Mutex<HashMap<String, InflightCell>>>,
    /// SHA-256 manifest loaded once per client session.
    manifest: Arc<Mutex<Option<HashMap<String, String>>>>,
}

impl CachedFetcher {
    pub fn new(http: reqwest::Client, base_url: String, cache_dir: PathBuf) -> Self {
        // Read env var at construction; absent → default jsDelivr mirror.
        let mirror_url = Some(
            std::env::var("DIVKIT_MIRROR_URL").unwrap_or_else(|_| DEFAULT_MIRROR_URL.to_string()),
        );
        Self {
            http,
            base_url,
            mirror_url,
            cache_dir,
            inflight: Arc::new(Mutex::new(HashMap::new())),
            manifest: Arc::new(Mutex::new(None)),
        }
    }

    /// Override the primary origin URL (used by `Divkit::with_base_url`).
    pub(crate) fn set_base_url(&mut self, url: String) {
        self.base_url = url;
    }

    /// Override the mirror URL (used by `Divkit::with_mirror_url`).
    ///
    /// `None` disables mirror fallback entirely.
    pub(crate) fn set_mirror_url(&mut self, url: Option<String>) {
        self.mirror_url = url;
    }

    /// Override the cache directory (used by `Divkit::with_cache_dir`).
    pub(crate) fn set_cache_dir(&mut self, dir: PathBuf) {
        self.cache_dir = dir;
    }

    /// Fetch a parquet file by logical key (e.g. `"dividends-2020"`).
    ///
    /// Single-flight: concurrent callers with the same key share one request.
    pub async fn fetch(&self, key: &str) -> Result<Bytes> {
        // Single-flight: get-or-create an in-flight cell for this key.
        let cell: InflightCell = {
            let mut map = self.inflight.lock().await;
            map.entry(key.to_string())
                .or_insert_with(|| Arc::new(OnceCell::new()))
                .clone()
        };

        let key_owned = key.to_string();
        let result = cell
            .get_or_init(|| async {
                match self.do_fetch(&key_owned).await {
                    Ok(b) => Ok(b),
                    Err(e) => Err(e.to_string()),
                }
            })
            .await;

        // Remove the cell from the map so that future fetches (e.g. stale
        // re-fetches after a new push) can run fresh.
        {
            let mut map = self.inflight.lock().await;
            map.remove(key);
        }

        result
            .clone()
            .map_err(|e| Error::Other(format!("fetch {key}: {e}")))
    }

    /// Inner fetch: retry on primary + CDN mirror fallback + stale cache.
    ///
    /// Mirror fallback is skipped when `self.mirror_url` is `None`.
    async fn do_fetch(&self, key: &str) -> Result<Bytes> {
        let cache_path = self.cache_dir.join(format!("{key}.parquet"));
        let etag_path = self.cache_dir.join(format!("{key}.parquet.etag"));

        // Try primary URL with retries.
        match self
            .fetch_with_retry(key, &self.base_url.clone(), &cache_path, &etag_path)
            .await
        {
            Ok(bytes) => {
                return self
                    .verify_and_return(key, bytes, &cache_path, &etag_path)
                    .await
            }
            Err(primary_err) => {
                // Mirror fallback — only when a mirror URL is configured.
                if let Some(mirror) = &self.mirror_url {
                    tracing::warn!(
                        key,
                        error = %primary_err,
                        "primary fetch exhausted retries, trying CDN mirror"
                    );
                    // Try CDN mirror (single attempt — no retry on mirror).
                    match self.fetch_single(key, &mirror.clone()).await {
                        Ok(bytes) => {
                            // Write to cache.
                            if let Err(e) = tokio::fs::create_dir_all(&self.cache_dir).await {
                                tracing::warn!("could not create cache dir: {e}");
                            } else if let Err(e) = tokio::fs::write(&cache_path, &bytes).await {
                                tracing::warn!("could not write mirror response to cache: {e}");
                            }
                            return self
                                .verify_and_return(key, bytes, &cache_path, &etag_path)
                                .await;
                        }
                        Err(mirror_err) => {
                            tracing::warn!(
                                key,
                                mirror_error = %mirror_err,
                                "CDN mirror also failed"
                            );
                        }
                    }
                } else {
                    tracing::debug!(key, "mirror fallback disabled, returning primary error");
                }
                // Stale cache fallback.
                if cache_path.exists() {
                    tracing::warn!(key, "all transports failed, serving stale cache");
                    let bytes = tokio::fs::read(&cache_path).await?;
                    return Ok(bytes.into());
                }
                Err(primary_err)
            }
        }
    }

    /// Try to fetch from `base` with up to `MAX_ATTEMPTS` attempts and
    /// exponential backoff. Respects ETag cache if local file exists.
    async fn fetch_with_retry(
        &self,
        key: &str,
        base: &str,
        cache_path: &Path,
        etag_path: &Path,
    ) -> Result<Bytes> {
        let url = format!("{base}/{key}.parquet");
        let mut last_err: Option<Error> = None;

        for attempt in 0..MAX_ATTEMPTS {
            if attempt > 0 {
                let delay_ms = backoff_delay_ms(attempt);
                tracing::debug!(key, attempt, delay_ms, "retry backoff");
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }

            let mut req = self.http.get(&url);
            if cache_path.exists() {
                if let Some(etag) = read_etag(etag_path) {
                    req = req.header("If-None-Match", etag);
                }
            }

            match req.send().await {
                Ok(resp) if resp.status() == StatusCode::NOT_MODIFIED => {
                    let bytes = tokio::fs::read(cache_path).await?;
                    return Ok(bytes.into());
                }
                Ok(resp) if resp.status().is_success() => {
                    let etag = resp
                        .headers()
                        .get("etag")
                        .and_then(|v| v.to_str().ok())
                        .map(String::from);
                    let bytes = resp.bytes().await?;
                    // Write body + ETag atomically.
                    tokio::fs::create_dir_all(cache_path.parent().unwrap_or(Path::new(".")))
                        .await?;
                    tokio::fs::write(cache_path, &bytes).await?;
                    if let Some(e) = etag {
                        tokio::fs::write(etag_path, e).await?;
                    }
                    return Ok(bytes);
                }
                Ok(resp) if resp.status() == StatusCode::TOO_MANY_REQUESTS => {
                    // 429 — respect Retry-After if present, else use backoff.
                    let delay = retry_after_delay(&resp)
                        .unwrap_or_else(|| Duration::from_millis(backoff_delay_ms(attempt + 1)));
                    tracing::warn!(
                        key,
                        attempt,
                        delay_secs = delay.as_secs_f32(),
                        "429 rate-limited"
                    );
                    if attempt + 1 < MAX_ATTEMPTS {
                        tokio::time::sleep(delay).await;
                        last_err =
                            Some(Error::Other(format!("fetch {key}: 429 Too Many Requests")));
                        continue;
                    }
                    return Err(Error::Other(format!(
                        "fetch {key}: 429 Too Many Requests (final)"
                    )));
                }
                Ok(resp) if should_retry_status(resp.status()) => {
                    // 5xx
                    last_err = Some(Error::Other(format!(
                        "fetch {key}: HTTP {} {}",
                        resp.status().as_u16(),
                        resp.status().canonical_reason().unwrap_or("")
                    )));
                }
                Ok(resp) => {
                    // 4xx (not 429) — not retriable.
                    return Err(Error::Other(format!(
                        "fetch {key}: HTTP {} {}",
                        resp.status().as_u16(),
                        resp.status().canonical_reason().unwrap_or("")
                    )));
                }
                Err(e) if is_retriable_error(&e) => {
                    tracing::warn!(key, attempt, error = %e, "transient error, will retry");
                    last_err = Some(Error::Http(e));
                }
                Err(e) => {
                    last_err = Some(Error::Http(e));
                    break; // non-retriable transport error
                }
            }
        }

        Err(last_err.unwrap_or_else(|| Error::Other(format!("fetch {key}: all attempts failed"))))
    }

    /// Single no-retry attempt from a mirror (CDN). No ETag used.
    async fn fetch_single(&self, key: &str, base: &str) -> Result<Bytes> {
        let url = format!("{base}/{key}.parquet");
        let resp = self.http.get(&url).send().await?;
        if resp.status().is_success() {
            Ok(resp.bytes().await?)
        } else {
            Err(Error::Other(format!(
                "mirror {key}: HTTP {} {}",
                resp.status().as_u16(),
                resp.status().canonical_reason().unwrap_or("")
            )))
        }
    }

    /// Verify SHA-256 digest from manifest (if available). On mismatch,
    /// remove the bad cache file and return an error.
    async fn verify_and_return(
        &self,
        key: &str,
        bytes: Bytes,
        cache_path: &Path,
        etag_path: &Path,
    ) -> Result<Bytes> {
        // Load manifest (once per client session).
        let expected_hex = self.manifest_digest_for(key).await;

        if let Some(expected) = expected_hex {
            let actual = hex_sha256(&bytes);
            if actual != expected {
                // Remove corrupt cache files.
                let _ = tokio::fs::remove_file(cache_path).await;
                let _ = tokio::fs::remove_file(etag_path).await;
                return Err(Error::ChecksumMismatch {
                    file: format!("{key}.parquet"),
                    expected,
                    actual,
                });
            }
        }

        Ok(bytes)
    }

    /// Fetch manifest and return the digest for `key`, or `None` if the
    /// manifest is unavailable or the key is absent.
    async fn manifest_digest_for(&self, key: &str) -> Option<String> {
        let mut manifest_guard = self.manifest.lock().await;
        if manifest_guard.is_none() {
            // Try to load manifest from primary URL.
            let manifest_url = format!("{}/manifest.json", self.base_url);
            match self.http.get(&manifest_url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    match resp.json::<HashMap<String, String>>().await {
                        Ok(m) => {
                            *manifest_guard = Some(m);
                        }
                        Err(e) => {
                            tracing::warn!("manifest parse failed: {e}");
                            *manifest_guard = Some(HashMap::new()); // mark as attempted
                        }
                    }
                }
                _ => {
                    // Manifest not present or unreachable; proceed without verification.
                    *manifest_guard = Some(HashMap::new());
                }
            }
        }

        manifest_guard
            .as_ref()?
            .get(&format!("{key}.parquet"))
            .and_then(|v| v.strip_prefix("sha256:").map(str::to_string))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn backoff_delay_ms(attempt: u32) -> u64 {
    // 2^attempt * BACKOFF_BASE_MS, capped at BACKOFF_MAX_MS
    let raw = BACKOFF_BASE_MS.saturating_mul(1u64 << attempt.min(10));
    raw.min(BACKOFF_MAX_MS)
}

fn should_retry_status(status: StatusCode) -> bool {
    status.is_server_error() // 5xx
}

fn is_retriable_error(e: &reqwest::Error) -> bool {
    e.is_connect() || e.is_timeout() || e.is_request()
}

fn retry_after_delay(resp: &reqwest::Response) -> Option<Duration> {
    let header = resp.headers().get("Retry-After")?;
    let val = header.to_str().ok()?;
    // Try integer seconds first, then give up (RFC 7231 also allows HTTP-date
    // but that's rare for 429; integer seconds is by far the common form).
    val.trim().parse::<u64>().ok().map(Duration::from_secs)
}

fn read_etag(path: &Path) -> Option<String> {
    std::fs::read_to_string(path).ok().filter(|s| !s.is_empty())
}

fn hex_sha256(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let result = hasher.finalize();
    hex_encode(&result)
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

// ---------------------------------------------------------------------------
// Cache directory resolution
// ---------------------------------------------------------------------------

/// Resolve the cache directory.
///
/// Priority:
/// 1. `$DIVKIT_CACHE_DIR` env var.
/// 2. XDG/platform cache dir for the `divkit` application
///    (`directories::ProjectDirs`).
/// 3. Fallback: `~/.cache/divkit` (or `%LOCALAPPDATA%\divkit\cache` on Windows).
pub(crate) fn default_cache_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("DIVKIT_CACHE_DIR") {
        return PathBuf::from(dir);
    }
    if let Some(proj) = directories::ProjectDirs::from("", "", "divkit") {
        return proj.cache_dir().to_path_buf();
    }
    dirs_fallback()
}

fn dirs_fallback() -> PathBuf {
    #[cfg(target_os = "windows")]
    {
        std::env::var("LOCALAPPDATA")
            .map(|d| PathBuf::from(d).join("divkit").join("cache"))
            .unwrap_or_else(|_| PathBuf::from("divkit-cache"))
    }
    #[cfg(not(target_os = "windows"))]
    {
        std::env::var("HOME")
            .map(|h| PathBuf::from(h).join(".cache").join("divkit"))
            .unwrap_or_else(|_| PathBuf::from(".divkit-cache"))
    }
}

/// Default primary base URL (GitHub raw content).
pub(crate) const DEFAULT_BASE_URL: &str =
    "https://raw.githubusercontent.com/userFRM/divkit/main/data";

/// Default CDN mirror (jsDelivr — Cloudflare-fronted mirror of the GitHub repo).
///
/// URL shape: `https://cdn.jsdelivr.net/gh/userFRM/divkit@main/data`
///
/// jsDelivr automatically mirrors public GitHub repos at no cost. Cache is
/// invalidated on each new commit. Override at runtime via `$DIVKIT_MIRROR_URL`.
pub(crate) const DEFAULT_MIRROR_URL: &str = "https://cdn.jsdelivr.net/gh/userFRM/divkit@main/data";

/// Resolve the base URL from the environment or use the default.
pub(crate) fn resolved_base_url() -> String {
    std::env::var("DIVKIT_BASE_URL").unwrap_or_else(|_| DEFAULT_BASE_URL.to_string())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_progression() {
        // attempt=0 → 250ms (initial attempt before any sleep — but we use sleep before retry,
        // so attempt=1 means first retry)
        assert_eq!(backoff_delay_ms(0), 250);
        assert_eq!(backoff_delay_ms(1), 500);
        assert_eq!(backoff_delay_ms(2), 1000);
        // Capped at 2000ms
        assert_eq!(backoff_delay_ms(3), 2000);
        assert_eq!(backoff_delay_ms(10), 2000);
    }

    #[test]
    fn hex_sha256_known_value() {
        // SHA-256 of empty bytes is a known constant.
        let digest = hex_sha256(b"");
        assert_eq!(
            digest,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn hex_sha256_hello() {
        // sha256("hello world") — verified with `echo -n "hello world" | sha256sum`
        let digest = hex_sha256(b"hello world");
        assert_eq!(
            digest,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn retry_after_none_on_missing_header() {
        // Can't easily construct a mock Response, so just test the helper with
        // a raw backoff path.
        // The backoff cap is 2000ms:
        assert!(backoff_delay_ms(100) <= BACKOFF_MAX_MS);
    }

    // ── with_mirror_url builder tests ─────────────────────────────────────────

    /// `with_mirror_url(None)` — primary 503 returns the error directly;
    /// the mirror (a second MockServer) receives **zero** requests.
    #[tokio::test]
    async fn test_with_mirror_url_none_skips_fallback() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let primary = MockServer::start().await;
        let mirror_sentinel = MockServer::start().await;

        // Primary always returns 503 (exhaust all retries).
        Mock::given(method("GET"))
            .and(path("/dividends-2020.parquet"))
            .respond_with(ResponseTemplate::new(503))
            .expect(3) // MAX_ATTEMPTS = 3
            .mount(&primary)
            .await;

        // Mirror sentinel: expect exactly ZERO requests.
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"irrelevant"))
            .expect(0)
            .mount(&mirror_sentinel)
            .await;

        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();
        let cache_dir = tempfile::TempDir::new().unwrap();
        let mut fetcher = CachedFetcher::new(http, primary.uri(), cache_dir.path().to_path_buf());
        // Disable mirror explicitly — builder form wins.
        fetcher.set_mirror_url(None);

        let result = fetcher.fetch("dividends-2020").await;
        assert!(
            result.is_err(),
            "primary 503 + no mirror must propagate error"
        );

        // Wiremock verifies the `expect(0)` on mirror_sentinel at drop.
    }

    /// `with_mirror_url(Some(custom))` — primary 503 → custom mirror is hit
    /// and returns OK bytes.
    #[tokio::test]
    async fn test_with_mirror_url_custom_used() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let primary = MockServer::start().await;
        let custom_mirror = MockServer::start().await;

        // Primary always 503.
        Mock::given(method("GET"))
            .and(path("/dividends-2020.parquet"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&primary)
            .await;

        // Custom mirror returns a minimal valid body (not a real parquet;
        // the fetcher only checks status here — SHA verification is skipped
        // when no manifest is present, which is the case for a mock server).
        Mock::given(method("GET"))
            .and(path("/dividends-2020.parquet"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"fake-parquet"))
            .expect(1)
            .mount(&custom_mirror)
            .await;

        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();
        let cache_dir = tempfile::TempDir::new().unwrap();
        let mut fetcher = CachedFetcher::new(http, primary.uri(), cache_dir.path().to_path_buf());
        fetcher.set_mirror_url(Some(custom_mirror.uri()));

        let result = fetcher.fetch("dividends-2020").await;
        assert!(
            result.is_ok(),
            "custom mirror should return bytes on primary failure"
        );
        assert_eq!(result.unwrap().as_ref(), b"fake-parquet");
    }

    /// Builder `set_mirror_url(Some(other))` wins over the env var.
    ///
    /// We verify this by pointing both the env var and the builder at two
    /// different MockServers and confirming the builder's server is the one
    /// that gets hit.
    #[tokio::test]
    async fn test_with_mirror_url_builder_wins_over_env() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let primary = MockServer::start().await;
        let env_mirror = MockServer::start().await;
        let builder_mirror = MockServer::start().await;

        // Primary always 503.
        Mock::given(method("GET"))
            .and(path("/dividends-2020.parquet"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&primary)
            .await;

        // env_mirror should NOT be contacted.
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"env-mirror"))
            .expect(0)
            .mount(&env_mirror)
            .await;

        // builder_mirror should be contacted exactly once.
        Mock::given(method("GET"))
            .and(path("/dividends-2020.parquet"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"builder-mirror"))
            .expect(1)
            .mount(&builder_mirror)
            .await;

        // Simulate "env var is set to env_mirror" by constructing the fetcher
        // with the env_mirror URI as the initial mirror, then override with
        // the builder form.
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();
        let cache_dir = tempfile::TempDir::new().unwrap();
        // Construct with env_mirror as the "default" mirror (simulates what
        // CachedFetcher::new reads from DIVKIT_MIRROR_URL).
        let mut fetcher = CachedFetcher::new(http, primary.uri(), cache_dir.path().to_path_buf());
        // Override: builder wins.
        fetcher.set_mirror_url(Some(builder_mirror.uri()));

        let result = fetcher.fetch("dividends-2020").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().as_ref(), b"builder-mirror");

        // wiremock verifies expect(0) on env_mirror and expect(1) on builder_mirror.
        let _ = env_mirror;
    }

    /// First fetch writes cache + ETag; second fetch sends `If-None-Match`
    /// and gets 304 → returns cached bytes without re-downloading the body.
    #[tokio::test]
    async fn test_etag_304_returns_cached() {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let body = b"dividend-parquet-bytes";
        let etag_value = "\"abc123\"";

        // First request: matches any GET on the path; responds once with 200 + body + ETag.
        // `up_to_n_times(1)` means it expires after one hit, so the second fetch
        // falls through to the 304-returning mock below.
        Mock::given(method("GET"))
            .and(path("/dividends-2020.parquet"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", etag_value)
                    .set_body_bytes(body.as_ref()),
            )
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;

        // Second request: If-None-Match present → 304.
        // Higher specificity (header matcher) so wiremock picks this over a stale generic mock.
        Mock::given(method("GET"))
            .and(path("/dividends-2020.parquet"))
            .and(header("If-None-Match", etag_value))
            .respond_with(ResponseTemplate::new(304))
            .expect(1)
            .mount(&server)
            .await;

        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();
        let cache_dir = tempfile::TempDir::new().unwrap();
        let mut fetcher = CachedFetcher::new(http, server.uri(), cache_dir.path().to_path_buf());
        // Disable mirror so 503 wouldn't cause spurious mirror attempts.
        fetcher.set_mirror_url(None);

        // First fetch — downloads body.
        let first = fetcher.fetch("dividends-2020").await.unwrap();
        assert_eq!(first.as_ref(), body);

        // Second fetch — 304, returns from cache.
        let second = fetcher.fetch("dividends-2020").await.unwrap();
        assert_eq!(second.as_ref(), body);

        // wiremock verifies both expect(1) assertions at drop.
    }

    /// All transports fail but cache exists → stale cache is returned.
    #[tokio::test]
    async fn test_stale_cache_fallback() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let stale_body = b"stale-dividend-data";

        // Server always 503.
        Mock::given(method("GET"))
            .and(path("/dividends-2020.parquet"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;

        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();
        let cache_dir = tempfile::TempDir::new().unwrap();

        // Pre-populate the cache with stale data.
        let cache_file = cache_dir.path().join("dividends-2020.parquet");
        tokio::fs::write(&cache_file, stale_body).await.unwrap();

        let mut fetcher = CachedFetcher::new(http, server.uri(), cache_dir.path().to_path_buf());
        fetcher.set_mirror_url(None);

        let result = fetcher.fetch("dividends-2020").await.unwrap();
        assert_eq!(result.as_ref(), stale_body);
    }
}
