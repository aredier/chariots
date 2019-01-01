use super::signatures;
use super::runners;

/// an an op that updadtes the Runner's data and the signs the runner
pub trait AbstractOp<D, S: signatures::AbstractSignature> {

    /// entry point of an op
    fn run_batch<R: runners::AbstractRunner> (&self, mut runner: R ) -> R {
        let batch_result: D = self.call(runner.data_batch());
        runner.set_result(batch_result, self.signature());
        runner
    }

    /// produces the signature of the op
    fn signature(&self) -> S;

    /// updates a batch of run
    fn  call (&self, data: D) -> D;
}
