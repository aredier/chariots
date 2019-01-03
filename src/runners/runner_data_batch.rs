use super::*;

/// a batch given to a runnerIterator if it's the first batch, than it is the meta data
/// otherwise it is the data of the batch itself
#[derive(Debug)]
pub enum RunnerDataBatch<DataType: Sized, OpSignatureType: signatures::Signature + Clone> {
    MetaData(RunnerMetaData<OpSignatureType>),
    Batch(DataType),
}
