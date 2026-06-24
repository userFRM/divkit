//! `PriceProvider` — caller-supplied spot price source for precomputed yield.
//! divkit ships no price feed; wire this to your own market-data/quote source.
use crate::Result;

pub trait PriceProvider {
    fn spot<'a>(
        &'a self,
        ticker: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<f64>> + Send + 'a>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Concept, DivEvent, DividendSnapshot};

    struct Fixed(f64);
    impl PriceProvider for Fixed {
        fn spot<'a>(
            &'a self,
            _t: &'a str,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::Result<f64>> + Send + 'a>>
        {
            let v = self.0;
            Box::pin(async move { Ok(v) })
        }
    }

    #[tokio::test]
    async fn yield_with_uses_provider_price() {
        // 4 recent quarterly payments so annual_amount() (today-anchored, but
        // the window is anchored to the last reported dividend) is a nonzero
        // 1.94 and the gate passes regardless of when the test runs.
        let today = chrono::Utc::now().date_naive();
        let mk = |days_ago: i64, amt: f64| {
            let d = today - chrono::Duration::days(days_ago);
            DivEvent {
                period_start: d,
                period_end: d,
                amount: amt,
                concept: Concept::Declared,
                accn: "x".into(),
                form: None,
            }
        };
        let snap = DividendSnapshot::from_events(
            "KO".into(),
            21344,
            vec![
                mk(280, 0.485),
                mk(190, 0.485),
                mk(100, 0.485),
                mk(10, 0.485),
            ],
        );
        // annual_amount() = 1.94 (4 payments within the trailing year ending at
        // the most recent dividend, which is 10 days ago → gate passes).
        let expected = 1.94 / 50.0;
        let y = snap.yield_on(50.0);
        assert!((y - expected).abs() < 1e-9, "yield_on mismatch: {y}");
        // yield_with must route the provider's price into the same calculation.
        let y_via_provider = snap.yield_with(&Fixed(50.0)).await.unwrap();
        assert!(
            (y_via_provider - expected).abs() < 1e-9,
            "yield_with mismatch: {y_via_provider}"
        );
    }
}
