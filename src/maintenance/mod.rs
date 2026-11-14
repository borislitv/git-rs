//! Elected source maintenance. Administrative requests only enqueue durable jobs;
//! source mutations are performed by the independently owned fenced worker.

mod compaction;
mod gc;
pub mod leader;
pub mod queue;
mod runtime;

pub use runtime::{Options, run};

enum Outcome {
    /// Journal and terminal job transition committed atomically.
    Committed,
    /// The controller must still commit the terminal job transition.
    Complete,
    Dead(&'static str),
}
