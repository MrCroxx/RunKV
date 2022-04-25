use std::time::{Duration, SystemTime, UNIX_EPOCH};

lazy_static::lazy_static! {
    /// 2022-05-09T00:00:00Z.
    static ref RUNKV_UNIX_DATE_EPOCH: SystemTime = SystemTime::UNIX_EPOCH + Duration::from_secs(1_620_518_400);
}

pub fn timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

pub fn rtimestamp() -> u64 {
    RUNKV_UNIX_DATE_EPOCH.elapsed().unwrap().as_millis() as u64
}

#[cfg(test)]
mod tests {
    use chrono::{Local, TimeZone, Utc};
    use test_log::test;

    use super::*;

    #[test]
    fn test_singularity_system_time() {
        let utc = Utc.ymd(2021, 5, 9).and_hms(0, 0, 0);
        let runkv_dt = Local.from_utc_datetime(&utc.naive_utc());
        let runkv_st = SystemTime::from(runkv_dt);
        assert_eq!(runkv_st, *RUNKV_UNIX_DATE_EPOCH);
    }
}
