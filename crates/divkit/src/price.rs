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
    use chrono::NaiveDate;

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
        let d = NaiveDate::parse_from_str("2024-12-13", "%Y-%m-%d").unwrap();
        let snap = DividendSnapshot::from_events(
            "KO".into(),
            21344,
            vec![DivEvent {
                period_start: d,
                period_end: d,
                amount: 1.94,
                concept: Concept::Declared,
                accn: "x".into(),
                form: None,
            }],
        );
        // single event → annual_amount fallback = recent (Irregular) = 1.94
        let y = snap.yield_with(&Fixed(50.0)).await.unwrap();
        assert!((y - 1.94 / 50.0).abs() < 1e-9);
    }
}
