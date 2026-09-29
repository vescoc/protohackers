use std::future;
use std::time::Duration;

use tracing::{info, instrument};

use wasi_async::time::{self, Instant};

pub(crate) struct Heartbeat {
    interval: Option<time::Interval>,
    period: Option<Duration>,
}

impl Heartbeat {
    pub(crate) fn new() -> Self {
        Self {
            period: None,
            interval: None,
        }
    }

    #[instrument(skip(self))]
    pub(crate) fn set_period(&mut self, period: Duration) {
        info!("setting period {period:?}");
        self.period = Some(period);
        if period == Duration::from_millis(0) {
            self.interval = None;
        } else {
            self.interval = Some(time::interval_at(Instant::now() + period, period));
        }
    }

    pub(crate) async fn tick(&mut self) {
        if let Some(interval) = self.interval.as_mut() {
            interval.tick().await;
        } else {
            future::pending::<()>().await;
        }
    }

    pub(crate) fn is_setted(&self) -> bool {
        self.period.is_some()
    }
}
