use std::pin::Pin;
use std::task::{Poll, Context};

use futures::{Sink, Stream};

#[allow(warnings)]
mod bindings;

mod poller;
mod reactor;
pub mod sync;

pub use reactor::Reactor;

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

pub struct SinkWrapper<Item, Error>(pub Pin<Box<dyn Sink<Item, Error = Error>>>);

impl<Item, Error> Sink<Item> for SinkWrapper<Item, Error> {
    type Error = Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut this = self.as_mut();

        this.0.as_mut().poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Item) -> Result<(), Self::Error> {
        let this = self.get_mut();

        this.0.as_mut().start_send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();

        this.0.as_mut().poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();

        this.0.as_mut().poll_close(cx)
    }
}

pub struct StreamWrapper<Item>(pub Pin<Box<dyn Stream<Item = Item>>>);

impl<Item> Stream for StreamWrapper<Item> {
    type Item = Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        this.0.as_mut().poll_next(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Once;

    pub(crate) fn init_tracing_subscriber() {
        static INIT_TRACING_SUBSCRIBER: Once = Once::new();
        INIT_TRACING_SUBSCRIBER.call_once(tracing_subscriber::fmt::init);
    }
}
