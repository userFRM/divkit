//! Dividend event and snapshot types with trailing-year and yield math.
use chrono::{Duration, NaiveDate};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Concept { Declared, CashPaid }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Frequency { Quarterly, SemiAnnual, Annual, Irregular, None }

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DivEvent {
    pub period_start: NaiveDate,
    pub period_end: NaiveDate,
    pub amount: f64,
    pub concept: Concept,
    pub accn: String,
    pub form: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DividendSnapshot {
    pub ticker: String,
    pub cik: u32,
    pub history: Vec<DivEvent>, // ascending by period_end
}

impl DividendSnapshot {
    pub fn from_events(ticker: String, cik: u32, mut events: Vec<DivEvent>) -> Self {
        events.sort_by_key(|e| e.period_end);
        Self { ticker, cik, history: events }
    }

    /// Distinct period_end events (dedup keeps first = Declared-preferred upstream).
    fn distinct(&self) -> Vec<&DivEvent> {
        let mut seen = std::collections::HashSet::new();
        self.history.iter().filter(|e| seen.insert(e.period_end)).collect()
    }

    pub fn frequency(&self) -> Frequency {
        let ev = self.distinct();
        if ev.is_empty() { return Frequency::None; }
        if ev.len() == 1 { return Frequency::Irregular; }
        // median spacing in days between consecutive distinct period_ends
        let mut gaps: Vec<i64> = ev.windows(2)
            .map(|w| (w[1].period_end - w[0].period_end).num_days()).collect();
        gaps.sort_unstable();
        let med = gaps[gaps.len() / 2];
        match med {
            d if d <= 135 => Frequency::Quarterly,
            d if d <= 225 => Frequency::SemiAnnual,
            d if d <= 450 => Frequency::Annual,
            _ => Frequency::Irregular,
        }
    }

    pub fn annual_amount(&self) -> f64 {
        let ev = self.distinct();
        if ev.is_empty() { return 0.0; }
        let last = ev.last().unwrap().period_end;
        let cutoff = last - Duration::days(365);
        let trailing: f64 = ev.iter()
            .filter(|e| e.period_end > cutoff && e.period_end <= last)
            .map(|e| e.amount).sum();
        if trailing > 0.0 { return trailing; }
        // Fallback: annualize the most-recent amount by inferred frequency.
        let recent = ev.last().unwrap().amount;
        match self.frequency() {
            Frequency::Quarterly => recent * 4.0,
            Frequency::SemiAnnual => recent * 2.0,
            Frequency::Annual => recent,
            _ => recent,
        }
    }

    pub fn yield_on(&self, price: f64) -> f64 {
        if price <= 0.0 { return 0.0; }
        self.annual_amount() / price
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn ev(end: &str, amt: f64) -> DivEvent {
        let d = NaiveDate::parse_from_str(end, "%Y-%m-%d").unwrap();
        DivEvent { period_start: d, period_end: d, amount: amt,
            concept: Concept::Declared, accn: "x".into(), form: None }
    }

    #[test]
    fn annual_amount_sums_trailing_year() {
        // 4 quarterly dividends within the trailing 365d window from the last one
        let snap = DividendSnapshot::from_events("KO".into(), 21344,
            vec![ev("2024-03-15",0.485), ev("2024-06-14",0.485),
                 ev("2024-09-13",0.485), ev("2024-12-13",0.485)]);
        assert!((snap.annual_amount() - 1.94).abs() < 1e-9);
    }

    #[test]
    fn frequency_quarterly_detected() {
        let snap = DividendSnapshot::from_events("KO".into(), 21344,
            vec![ev("2024-03-15",0.485), ev("2024-06-14",0.485),
                 ev("2024-09-13",0.485), ev("2024-12-13",0.485)]);
        assert_eq!(snap.frequency(), Frequency::Quarterly);
    }

    #[test]
    fn non_payer_is_zero_and_none() {
        let snap = DividendSnapshot::from_events("XYZ".into(), 1, vec![]);
        assert_eq!(snap.annual_amount(), 0.0);
        assert_eq!(snap.frequency(), Frequency::None);
        assert_eq!(snap.yield_on(100.0), 0.0);
    }

    #[test]
    fn yield_on_divides_amount_by_price() {
        let snap = DividendSnapshot::from_events("KO".into(), 21344,
            vec![ev("2024-03-15",0.485), ev("2024-06-14",0.485),
                 ev("2024-09-13",0.485), ev("2024-12-13",0.485)]);
        let y = snap.yield_on(50.0);
        assert!((y - (1.94/50.0)).abs() < 1e-9);
        assert_eq!(snap.yield_on(0.0), 0.0);
    }
}
