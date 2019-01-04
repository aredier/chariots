/// module that provides wrappers around iterators in order to carry some metadata
/// (at th present time the signatures of the ops that ran it) along the data
mod runner;
mod runner_iterator;
mod runner_metadata;
mod runner_data_batch;
mod tests;

use std::clone::Clone;

use super::signatures;
use super::ops;

pub use self::runner::Runner;
pub use self::runner_metadata::RunnerMetaData;
pub use self::runner_data_batch::RunnerDataBatch;
pub use self::runner_iterator::RunnerIterator;


#[derive(Debug)]
pub struct NoMetaDAtaError;
