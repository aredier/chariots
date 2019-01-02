use super::runners;

/// an an op that updadtes the Runner's data and the signs the runner
pub trait AbstractOp {
    type InputDataType;
    type Runnertype: runners::AbstractRunner;
    type OutputDatatype;

    /// entry point of an op
    fn run_batch(&self, mut runner: Self::Runnertype) -> Self::Runnertype {
        let (batch_input, data_signature): (Self::InputDataType, Self::Runnertype::DataSignatureType) = runner.data_batch();
        let (batch_result: DataType, ) = self.call(runner.data_batch());
        runner.set_result(batch_result, self.signature());
        runner
    }

    /// produces the signature of the op
    fn signature(&self) -> OpSingaturetype;

    /// updates a batch of run
    fn  call (&self, data: DataType) -> DataType;
}
