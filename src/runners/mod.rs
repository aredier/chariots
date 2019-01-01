use super::signatures;

/// abstraction of a data that is passed to the ops
pub trait AbstractRunner {

    /// get the runners present batch of data
    fn data_batch<T> (&self) -> T;

    /// sets gives the result of an op as well as it's signature to the runner
    fn set_result<T, S: signatures::AbstractSignature> (&mut self, batch_result: T, signature: S);
}
