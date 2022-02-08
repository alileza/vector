mod limited_queue;
pub(self) mod poll_notify;
pub(self) mod poll_semaphore;
pub(crate) mod poll_sender;
mod receiver;
mod sender;
mod strategy;

pub use limited_queue::{limited, LimitedReceiver, LimitedSender};
pub use receiver::*;
pub use sender::*;
pub(self) use strategy::*;

#[cfg(test)]
mod tests;
