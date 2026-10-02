use std::fmt;
use std::future::Future;
use std::ops;
use std::time::Duration;

use tracing::{instrument, trace};

use wasi::clocks::monotonic_clock;

use wasi_async_runtime::Reactor;

#[derive(thiserror::Error)]
pub struct Elapsed<F>(pub F);

impl<F> fmt::Display for Elapsed<F> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(fmt, "Elapsed")
    }
}

impl<F> fmt::Debug for Elapsed<F> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(fmt, "Elapsed")
    }
}

/// # Errors
/// # Panics
#[instrument(skip_all)]
#[allow(clippy::cast_possible_truncation, clippy::let_and_return)]
pub async fn timeout<F: Future + Unpin>(duration: Duration, future: F) -> Result<F::Output, Elapsed<F>> {
    let reactor = Reactor::current().await;

    let subscription = monotonic_clock::subscribe_duration(duration.as_nanos() as u64);
    trace!("subscribe duration {subscription:?}");
    let wait_for = reactor.wait_for(subscription);

    let result = match futures::future::select(future, wait_for).await {
        futures::future::Either::Left((result, _)) => Ok(result),
        futures::future::Either::Right(((), future)) => Err(Elapsed(future)),
    };

    result
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Instant(u64);

impl Instant {
    #[must_use]
    pub fn now() -> Self {
        Self(monotonic_clock::now())
    }

    #[must_use]
    #[expect(clippy::unchecked_time_subtraction, reason = "I use monotonic clock")]
    pub fn elapsed(&self) -> Duration {
        Duration::from_nanos(monotonic_clock::now()) - Duration::from_nanos(self.0)
    }
}

impl ops::Add<Duration> for Instant {
    type Output = Instant;

    #[allow(clippy::cast_possible_truncation)]
    fn add(self, duration: Duration) -> Self::Output {
        Self(self.0 + duration.as_nanos() as u64)
    }
}

impl ops::AddAssign<Duration> for Instant {
    #[allow(clippy::cast_possible_truncation)]
    fn add_assign(&mut self, duration: Duration) {
        self.0 += duration.as_nanos() as u64;
    }
}

/// # Panics
#[must_use]
pub fn interval_at(start: Instant, period: Duration) -> Interval {
    assert_ne!(period, Duration::from_nanos(0));
    Interval {
        current: start,
        period,
    }
}

pub struct Interval {
    current: Instant,
    period: Duration,
}

impl Interval {
    #[must_use]
    pub fn period(&self) -> Duration {
        self.period
    }

    #[instrument(skip_all)]
    pub async fn tick(&mut self) {
        let subscription = monotonic_clock::subscribe_instant(self.current.0);
        trace!("subscribe instant {subscription:?}");
        Reactor::current().await.wait_for(subscription).await;
        self.current += self.period;
    }
}
