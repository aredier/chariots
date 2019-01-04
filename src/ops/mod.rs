/// module that provides the op trait(s) which are at the heart of chariots' framework
/// an op performs an action on each data batch and returns a signed signature
use super::signatures;


/// an an op that updadtes the Runner's data and signs the runner
pub trait AbstractOp {
    type OpSignatureType: signatures::Signature + Clone;
    type InputDataType: Sized;
    type OutputDataType: Sized;

    /// produces the signature of the op
    fn signature(&self) -> Self::OpSignatureType;

    /// performs the op on a batch
    fn  call (&self, data: Self::InputDataType) -> Self::OutputDataType;
}
