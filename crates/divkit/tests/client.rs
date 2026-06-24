//! Integration tests for the `Divkit` client.
//!
//! Spins up a local wiremock HTTP server serving a synthetic manifest and the
//! committed fixture parquet, then asserts end-to-end dividend retrieval.

use divkit::Divkit;
use tempfile::TempDir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// SHA-256 (hex) of `tests/fixtures/dividends-2024.parquet` at commit time.
///
/// Regenerate with: `sha256sum crates/divkit/tests/fixtures/dividends-2024.parquet`
const FIXTURE_SHA256: &str = "d0fe742c4c6de9147ed28e8bb85f82949361dbe64851603f1e2385fa1342ddd9";

/// Load the committed parquet fixture as bytes.
fn fixture_bytes() -> Vec<u8> {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    std::fs::read(manifest_dir.join("tests/fixtures/dividends-2024.parquet"))
        .expect("fixture parquet must exist — run `cargo test --test parquet_io make_fixture -- --ignored` first")
}

/// Build a manifest JSON body in the FLAT format the fetcher actually reads:
/// `{"<file>.parquet": "sha256:<hex>"}` — a `HashMap<String, String>` where the
/// value carries a `sha256:` prefix. The fetcher strips that prefix and compares
/// against the digest of the served bytes. A NESTED form (`{"sha256": "<hex>"}`)
/// would fail to deserialize and silently disable verification — so this shape
/// is load-bearing for the verification path to run at all.
fn manifest_body_with_digest(hex: &str) -> String {
    format!(r#"{{"dividends-2024.parquet": "sha256:{hex}"}}"#)
}

/// Manifest listing the shard with its CORRECT sha256 — verification passes.
fn manifest_body() -> String {
    manifest_body_with_digest(FIXTURE_SHA256)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Known-ticker path: KO has 4×$0.485 = $1.94 trailing annual dividend.
#[tokio::test]
async fn annual_dividend_known_ticker() {
    let server = MockServer::start().await;
    let parquet = fixture_bytes();

    // Serve manifest.json
    Mock::given(method("GET"))
        .and(path("/manifest.json"))
        .respond_with(ResponseTemplate::new(200).set_body_string(manifest_body()))
        .expect(1..)
        .mount(&server)
        .await;

    // Serve the parquet shard
    Mock::given(method("GET"))
        .and(path("/dividends-2024.parquet"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(parquet))
        .expect(1..)
        .mount(&server)
        .await;

    let cache_dir = TempDir::new().unwrap();
    let client = Divkit::new()
        .with_base_url(server.uri())
        .with_cache_dir(cache_dir.path().to_path_buf())
        .with_mirror_url(None); // disable CDN fallback in tests

    let annual = client.annual_dividend("KO").await.unwrap();
    assert!(
        annual.is_some(),
        "KO is in the fixture — annual_dividend must return Some(_)"
    );
    let amount = annual.unwrap();
    assert!((amount - 1.94).abs() < 1e-9, "expected ~1.94, got {amount}");
}

/// Unknown-ticker path: ticker absent from all shards → Ok(None).
#[tokio::test]
async fn annual_dividend_unknown_ticker_returns_none() {
    let server = MockServer::start().await;
    let parquet = fixture_bytes();

    Mock::given(method("GET"))
        .and(path("/manifest.json"))
        .respond_with(ResponseTemplate::new(200).set_body_string(manifest_body()))
        .expect(1..)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/dividends-2024.parquet"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(parquet))
        .expect(1..)
        .mount(&server)
        .await;

    let cache_dir = TempDir::new().unwrap();
    let client = Divkit::new()
        .with_base_url(server.uri())
        .with_cache_dir(cache_dir.path().to_path_buf())
        .with_mirror_url(None);

    let annual = client.annual_dividend("NOPE").await.unwrap();
    assert_eq!(annual, None, "unknown ticker must return Ok(None)");
}

/// `dividends` returns one `DivEvent` per matching row.
#[tokio::test]
async fn dividends_for_known_ticker() {
    let server = MockServer::start().await;
    let parquet = fixture_bytes();

    Mock::given(method("GET"))
        .and(path("/manifest.json"))
        .respond_with(ResponseTemplate::new(200).set_body_string(manifest_body()))
        .expect(1..)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/dividends-2024.parquet"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(parquet))
        .expect(1..)
        .mount(&server)
        .await;

    let cache_dir = TempDir::new().unwrap();
    let client = Divkit::new()
        .with_base_url(server.uri())
        .with_cache_dir(cache_dir.path().to_path_buf())
        .with_mirror_url(None);

    let events = client.dividends("KO").await.unwrap();
    assert_eq!(events.len(), 4, "fixture has 4 KO rows");
    for ev in &events {
        assert!((ev.amount - 0.485).abs() < 1e-9);
    }
}

/// `dividend_snapshot` builds a `DividendSnapshot` with the correct ticker and CIK.
#[tokio::test]
async fn dividend_snapshot_for_known_ticker() {
    let server = MockServer::start().await;
    let parquet = fixture_bytes();

    Mock::given(method("GET"))
        .and(path("/manifest.json"))
        .respond_with(ResponseTemplate::new(200).set_body_string(manifest_body()))
        .expect(1..)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/dividends-2024.parquet"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(parquet))
        .expect(1..)
        .mount(&server)
        .await;

    let cache_dir = TempDir::new().unwrap();
    let client = Divkit::new()
        .with_base_url(server.uri())
        .with_cache_dir(cache_dir.path().to_path_buf())
        .with_mirror_url(None);

    let snap = client.dividend_snapshot("KO").await.unwrap();
    assert_eq!(snap.ticker, "KO");
    assert_eq!(snap.cik, 21344);
    assert_eq!(snap.history.len(), 4);
    assert!((snap.annual_amount() - 1.94).abs() < 1e-9);
}

/// Blocking wrapper works from synchronous context.
#[test]
fn annual_dividend_blocking_known_ticker() {
    // Build a tokio runtime to host the mock server, then call the blocking wrapper.
    let rt = tokio::runtime::Runtime::new().unwrap();

    let server = rt.block_on(async { MockServer::start().await });
    let parquet = fixture_bytes();

    rt.block_on(async {
        Mock::given(method("GET"))
            .and(path("/manifest.json"))
            .respond_with(ResponseTemplate::new(200).set_body_string(manifest_body()))
            .expect(1..)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/dividends-2024.parquet"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(parquet))
            .expect(1..)
            .mount(&server)
            .await;
    });

    let cache_dir = TempDir::new().unwrap();
    let client = Divkit::new()
        .with_base_url(server.uri())
        .with_cache_dir(cache_dir.path().to_path_buf())
        .with_mirror_url(None);

    let annual = client.annual_dividend_blocking("KO").unwrap();
    assert!(annual.is_some());
    assert!((annual.unwrap() - 1.94).abs() < 1e-9);
}

/// Negative test: proves SHA-256 verification is actually active.
///
/// Serves the genuine fixture parquet but advertises a WRONG digest in the
/// manifest (all-zeros hex). The fetcher must detect the mismatch and surface
/// an error instead of returning the bytes. This guards against a regression
/// that silently disables verification (e.g. a manifest-shape mismatch).
#[tokio::test]
async fn checksum_mismatch_is_rejected() {
    use divkit::Error;

    let server = MockServer::start().await;
    let parquet = fixture_bytes();

    // Manifest advertises a digest that does NOT match the served bytes.
    let bad_digest = "0".repeat(64);
    Mock::given(method("GET"))
        .and(path("/manifest.json"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(manifest_body_with_digest(&bad_digest)),
        )
        .expect(1..)
        .mount(&server)
        .await;

    // Serve the genuine fixture — its real digest differs from the manifest's.
    Mock::given(method("GET"))
        .and(path("/dividends-2024.parquet"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(parquet))
        .expect(1..)
        .mount(&server)
        .await;

    let cache_dir = TempDir::new().unwrap();
    let client = Divkit::new()
        .with_base_url(server.uri())
        .with_cache_dir(cache_dir.path().to_path_buf())
        .with_mirror_url(None);

    let result = client.annual_dividend("KO").await;
    assert!(
        result.is_err(),
        "a digest mismatch must surface as an error, not be silently ignored"
    );

    // The error chain must report a checksum mismatch. The client wraps fetch
    // errors in `Error::Other` (via `fetch {key}: {e}` in the single-flight
    // path), so assert on the rendered message naming the mismatch.
    let err = result.unwrap_err();
    let msg = err.to_string();
    let is_checksum =
        matches!(err, Error::ChecksumMismatch { .. }) || msg.contains("checksum mismatch");
    assert!(
        is_checksum,
        "expected a checksum-mismatch error, got: {msg}"
    );
}
