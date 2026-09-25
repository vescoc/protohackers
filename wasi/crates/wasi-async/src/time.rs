use std::fmt;
use std::future::Future;
use std::ops;
use std::time::Duration;

use futures::FutureExt;
use futures_concurrency::prelude::*;

use tracing::{instrument, trace};

use wasi::clocks::monotonic_clock;

use wasi_async_runtime::Reactor;

#[derive(Debug, PartialEq)]
pub struct Elapsed;

impl std::error::Error for Elapsed {}

impl fmt::Display for Elapsed {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(fmt, "Elapsed")
    }
}

/// # Errors
/// # Panics
#[instrument(skip_all)]
#[allow(clippy::cast_possible_truncation)]
pub async fn timeout<F: Future>(
    reactor: Reactor,
    duration: Duration,
    future: F,
) -> Result<F::Output, Elapsed> {
    let subscription = monotonic_clock::subscribe_duration(duration.as_nanos() as u64);
    trace!("subscribe duration {subscription:?}");
    let wait_for = reactor.wait_for(subscription).map(|()| Err(Elapsed));

    let future = future.map(Ok);

    (wait_for, future).race().await
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Instant(u64);

impl Instant {
    #[must_use]
    pub fn now() -> Self {
        Self(monotonic_clock::now())
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
pub fn interval_at(reactor: Reactor, start: Instant, period: Duration) -> Interval {
    assert_ne!(period, Duration::from_nanos(0));
    Interval {
        reactor,
        current: start,
        period,
    }
}

pub struct Interval {
    reactor: Reactor,
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
        self.reactor.wait_for(subscription).await;
        self.current += self.period;
    }
}
