use super::runners;
use super::signatures;


/// an an op that updadtes the Runner's data and the signs the runner
pub trait AbstractOp {
    type OpSignatureType: signatures::Signature + Clone;
    type InputDataType: Sized;

    fn add_to<'a: 's, 's>(&'s mut self, mut runner: runners::Runner<'a, Self::InputDataType, Self::OpSignatureType>)
    -> Result<runners::Runner<'s, runners::RunnerDataBatch<Self::InputDataType, Self::OpSignatureType>, Self::OpSignatureType>, runners::NoMetaDAtaError>
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

    /// updates a batch of s
    fn  call (&self, data: Self::InputDataType) -> Self::InputDataType;
}
