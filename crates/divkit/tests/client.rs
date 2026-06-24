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

/// Known-ticker path: KO is present in the fixture; annual_dividend returns Some(_).
///
/// The fixture contains 2024 events. When queried today the trailing-365d window
/// relative to now may not include those events (the fixture is static), so we
/// assert `Some(_)` rather than a specific value. The exact trailing-sum logic is
/// covered by `record::tests::annual_amount_sums_trailing_year` using
/// `annual_amount_as_of` with a fixed anchor.
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
///
/// The fixture contains 4 KO events in 2024.  Use `annual_amount_as_of` anchored
/// to 2024-12-13 so the trailing-365d assertion is deterministic regardless of
/// when this test runs.
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
    // Anchor to the last fixture event date for a deterministic result.
    let as_of = chrono::NaiveDate::from_ymd_opt(2024, 12, 13).unwrap();
    assert!((snap.annual_amount_as_of(as_of) - 1.94).abs() < 1e-9);
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
    // KO is present in the 2024 fixture — blocking variant must return Some(_).
    // The exact sum depends on today's date (the trailing-365d window is live).
    // Value correctness is covered by record::tests::annual_amount_sums_trailing_year.
    assert!(annual.is_some());
}

/// Blocking wrapper from within a `#[tokio::test]` (current-thread) runtime.
///
/// `#[tokio::test]` uses a current-thread executor by default. Calling
/// `block_in_place` on a current-thread runtime panics; the `block()` helper
/// must detect the current-thread flavor and run the future on a fresh
/// dedicated OS thread instead.
#[tokio::test]
async fn annual_dividend_blocking_from_current_thread_runtime() {
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

    // This must NOT panic — a panic here means block() incorrectly called
    // block_in_place on the current-thread runtime.
    let annual = client.annual_dividend_blocking("KO").unwrap();
    assert!(
        annual.is_some(),
        "KO must be found even when blocking wrapper is called from current-thread runtime"
    );
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

/// Finding 1 regression guard: two CIKs share ticker "DUP".
///
/// CIK 1001 is the older issuer (latest period_end 2022-12-31, amount 0.10).
/// CIK 1002 is the current issuer (latest period_end 2024-12-31, amount 0.50).
///
/// `client.dividends("DUP")` must return only CIK 1002's rows, and the result
/// must equal `cache.dividends("DUP")`.
#[tokio::test]
async fn dividends_deduplicates_cross_cik_ticker_collision() {
    use divkit::parquet_io::{write_dividends, DivRow};
    use divkit::{Concept, DividendCache};
    use sha2::{Digest, Sha256};

    // Build a synthetic parquet with two CIKs sharing ticker "DUP".
    let rows = vec![
        // Older issuer — CIK 1001, last period_end 2022-12-31.
        DivRow {
            cik: 1001,
            ticker: Some("DUP".into()),
            period_start: chrono::NaiveDate::from_ymd_opt(2022, 1, 1).unwrap(),
            period_end: chrono::NaiveDate::from_ymd_opt(2022, 3, 31).unwrap(),
            amount: 0.10,
            concept: Concept::Declared,
            accn: "old-q1".into(),
            form: Some("10-Q".into()),
        },
        DivRow {
            cik: 1001,
            ticker: Some("DUP".into()),
            period_start: chrono::NaiveDate::from_ymd_opt(2022, 10, 1).unwrap(),
            period_end: chrono::NaiveDate::from_ymd_opt(2022, 12, 31).unwrap(),
            amount: 0.10,
            concept: Concept::Declared,
            accn: "old-q4".into(),
            form: Some("10-Q".into()),
        },
        // Current issuer — CIK 1002, last period_end 2024-12-31 (later → wins).
        DivRow {
            cik: 1002,
            ticker: Some("DUP".into()),
            period_start: chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            period_end: chrono::NaiveDate::from_ymd_opt(2024, 3, 31).unwrap(),
            amount: 0.50,
            concept: Concept::Declared,
            accn: "new-q1".into(),
            form: Some("10-Q".into()),
        },
        DivRow {
            cik: 1002,
            ticker: Some("DUP".into()),
            period_start: chrono::NaiveDate::from_ymd_opt(2024, 10, 1).unwrap(),
            period_end: chrono::NaiveDate::from_ymd_opt(2024, 12, 31).unwrap(),
            amount: 0.50,
            concept: Concept::Declared,
            accn: "new-q4".into(),
            form: Some("10-Q".into()),
        },
    ];

    let tmp_dir = tempfile::TempDir::new().unwrap();
    let parquet_path = tmp_dir.path().join("dividends-2024.parquet");
    write_dividends(&parquet_path, &rows).unwrap();
    let parquet_bytes = std::fs::read(&parquet_path).unwrap();

    let digest = {
        let mut h = Sha256::new();
        h.update(&parquet_bytes);
        h.finalize()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    };
    let manifest = format!(r#"{{"dividends-2024.parquet": "sha256:{digest}"}}"#);

    // Two servers: one for the client reference, one for the cache.
    let server_a = MockServer::start().await;
    let server_b = MockServer::start().await;
    for server in [&server_a, &server_b] {
        Mock::given(method("GET"))
            .and(path("/manifest.json"))
            .respond_with(ResponseTemplate::new(200).set_body_string(manifest.clone()))
            .expect(1..)
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path("/dividends-2024.parquet"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(parquet_bytes.clone()))
            .expect(1..)
            .mount(server)
            .await;
    }

    // Client path.
    let cache_dir_a = TempDir::new().unwrap();
    let client_a = Divkit::new()
        .with_base_url(server_a.uri())
        .with_cache_dir(cache_dir_a.path().to_path_buf())
        .with_mirror_url(None);
    let client_events = client_a.dividends("DUP").await.unwrap();

    // Must return only the current issuer's rows (CIK 1002, amount 0.50).
    assert_eq!(
        client_events.len(),
        2,
        "client.dividends(DUP) must return only CIK 1002's 2 rows; got {}",
        client_events.len()
    );
    for ev in &client_events {
        assert!(
            (ev.amount - 0.50).abs() < 1e-9,
            "all returned events must be from the current issuer (amount 0.50), got {}",
            ev.amount
        );
    }

    // Cache path must agree.
    let cache_dir_b = TempDir::new().unwrap();
    let client_b = Divkit::new()
        .with_base_url(server_b.uri())
        .with_cache_dir(cache_dir_b.path().to_path_buf())
        .with_mirror_url(None);
    let cache = DividendCache::hydrate_with(&client_b).await.unwrap();
    let cache_events = cache.dividends("DUP");

    assert_eq!(
        cache_events.len(),
        client_events.len(),
        "cache.dividends(DUP) must return the same count as client.dividends(DUP)"
    );
    for (ce, cc) in client_events.iter().zip(cache_events.iter()) {
        assert!(
            (ce.amount - cc.amount).abs() < 1e-9,
            "client and cache events must agree: client={} cache={}",
            ce.amount,
            cc.amount
        );
    }
}
