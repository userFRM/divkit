# divkit

US equity dividends and dividend yield for Rust, from SEC EDGAR public-domain XBRL.

```toml
[dependencies]
divkit = "0.0.3"
```

```rust,no_run
#[tokio::main]
async fn main() -> divkit::Result<()> {
    let annual = divkit::annual_dividend_for("KO").await?;
    println!("KO annual dividend: {annual:?}");
    Ok(())
}
```

Full documentation: <https://github.com/userFRM/divkit>

Licensed under MIT OR Apache-2.0.
