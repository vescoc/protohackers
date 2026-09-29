use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll};

use tracing::{instrument, trace};

#[allow(warnings)]
mod bindings;

mod poller;
mod reactor;
pub mod sync;

pub use reactor::Reactor;

/// Block on the passed future and wait completion.
#[instrument(skip_all)]
pub fn block_on<F, Fut>(f: F) -> Fut::Output
where
    F: FnOnce(Reactor) -> Fut,
    Fut: Future,
{
    let (reactor, waker) = Reactor::new();

    let fut = (f)(reactor.clone());
    let mut fut = pin!(fut);

    let mut cx = Context::from_waker(&waker);

    loop {
        trace!("block_on loop");

        // reactor.reset_main_task_state();
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(res) => return res,
            Poll::Pending => reactor.block_until(),
        }
    }
}

/// Yield now the execution
pub async fn yield_now() {
    let mut yielded = false;
    std::future::poll_fn(move |cx| {
        if yielded {
            std::task::Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    })
    .await;
}

#[cfg(test)]
mod tests {
    use std::sync::Once;

    pub(crate) fn init_tracing_subscriber() {
        static INIT_TRACING_SUBSCRIBER: Once = Once::new();
        INIT_TRACING_SUBSCRIBER.call_once(tracing_subscriber::fmt::init);
    }
}
