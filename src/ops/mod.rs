/// module that provides the op trait(s) which are at the heart of chariots' framework
/// an op performs an action on each data batch and returns a signed signature

use super::runners;
use super::signatures;


/// an an op that updadtes the Runner's data and signs the runner
pub trait AbstractOp {
    type OpSignatureType: signatures::Signature + Clone;
    type InputDataType: Sized;
    type OutputDataType: Sized;

    /// function that adds the op to the runner (consuming it) and returns a new runner
    /// with it's `call` method mapped to it
    fn add_to<'a: 's, 's>(&'s mut self, mut runner: runners::Runner<'a, Self::InputDataType, Self::OpSignatureType>)
    // TODO: fix clippy lint
    -> Result<runners::Runner<'s, Self::OutputDataType, Self::OpSignatureType>, runners::NoMetaDAtaError>
    {
        runner.sign(self.signature());
        let unwrap_and_map = move  |x| {
            match  x {
                runners::RunnerDataBatch::MetaData(meta_data) => {
                    runners::RunnerDataBatch::MetaData(meta_data)
                },
                runners::RunnerDataBatch::Batch(data) => {
                    runners::RunnerDataBatch::Batch(self.call(data))
                }
            }
        };
        runners::Runner::from_runner_iterator(runner.into_iter().map(unwrap_and_map))
    }

    /// produces the signature of the op
    fn signature(&self) -> Self::OpSignatureType;

    /// performs the op on a batch
    fn  call (&self, data: Self::InputDataType) -> Self::OutputDataType;
}
