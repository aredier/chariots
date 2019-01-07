use std::rc::Rc;

use super::*;

/// the actual runner that gets passed to the ops
pub struct Runner<'a, DataType: Sized, OpSignatureType: signatures::Signature + Clone + 'a> {
    meta_data: Rc<RunnerMetaData<OpSignatureType>>,
    data: Box<Iterator<Item=DataType> + 'a>,
}

impl<'a, DataType: Sized + 'a, OpSignatureType: signatures::Signature + Clone> Runner<'a, DataType, OpSignatureType> {

    /// produces a new runner with blank metadata
    pub fn new<'b: 'a, IntoIter: IntoIterator<Item=DataType>> (data: IntoIter) -> Self
    where IntoIter::IntoIter: 'b {
        Runner {data: Box::new(data.into_iter()), meta_data: RunnerMetaData::new()}
    }

    /// signs the runner with some OpSignature
    pub fn sign (&mut self, signature: OpSignatureType) {
        self.meta_data = RunnerMetaData::sign(&self.meta_data, signature);
    }

    /// creates a new runner from an existing metadata
    fn new_with_meta<'b: 'a, IntoIter: IntoIterator<Item=DataType>> (data: IntoIter, meta_data: Rc<RunnerMetaData<OpSignatureType>>) -> Self
    where IntoIter::IntoIter: 'b {
        Runner {data: Box::new(data.into_iter()), meta_data}
    }

    /// creates a new Runner from a DataTypeor that has a metadata as it's first element
    pub fn from_runner_iterator<'b, Iter: Iterator<Item=RunnerDataBatch<DataType, OpSignatureType>>> (mut iter: Iter, )
    ->  Result<Runner<'b, DataType, OpSignatureType>, NoMetaDAtaError>
    where Iter: 'b, DataType: 'b
     {
        let meta_data: Rc<RunnerMetaData<OpSignatureType>>;
        if let Some(RunnerDataBatch::MetaData(meta)) = iter.next(){
            meta_data = meta;
        } else {
            return Err(NoMetaDAtaError)
        }
        let res_iterator = iter.map(|x| {
            match x {
                RunnerDataBatch::Batch(data) => {data},
                RunnerDataBatch::MetaData(_) => {
                    // TODO get rid of panic
                    panic!("duplicate metadata error");
                }
            }
        });
        Ok(Runner::new_with_meta(res_iterator, meta_data))
    }

    pub fn chain<'b: 'a, O: ops::AbstractOp<InputDataType=DataType, OpSignatureType=OpSignatureType>> (self, op: &'b mut O)
    -> Result<Runner<'a, O::OutputDataType, OpSignatureType>, NoMetaDAtaError>
    where O: 'a
     {
        let signature = op.signature().clone();
        let unwrap_and_map = move |x| {
            match  x {
                RunnerDataBatch::MetaData(meta_data) => {
                    RunnerDataBatch::MetaData(meta_data)
                },
                RunnerDataBatch::Batch(data) => {
                    RunnerDataBatch::Batch(op.call(data))
                }
            }
        };
        let mut res = Runner::from_runner_iterator(self.into_iter().map(unwrap_and_map));
        if let Ok(res_unwraped) = res.as_mut() {
            res_unwraped.sign(signature);
        }
        res
    }

    pub fn merge<'b: 'a, OtherDataType: Sized> (self, other: Runner<'b, OtherDataType, OpSignatureType>)
    -> Result<Runner<'a, (DataType, OtherDataType), OpSignatureType>, NoMetaDAtaError>
    where OtherDataType: 'b
    {
        let mut self_iterator = self.into_iter();

        // TODO writ macro for this
        let self_meta_data: Rc<RunnerMetaData<OpSignatureType>>;
        if let Some(RunnerDataBatch::MetaData(meta)) = self_iterator.next(){
            self_meta_data = meta;
        } else {
            return Err(NoMetaDAtaError)
        }
        let self_unwrapped_iter = self_iterator.map(|x| {
            match x {
                RunnerDataBatch::Batch(data) => {data},
                RunnerDataBatch::MetaData(_) => {
                    // TODO get rid of panic
                    panic!("duplicate metadata error");
                }
            }
        });

        let mut other_iterator = other.into_iter();
        // TODO writ macro for this
        let other_meta_data: Rc<RunnerMetaData<OpSignatureType>>;
        if let Some(RunnerDataBatch::MetaData(meta)) = other_iterator.next(){
            other_meta_data = meta;
        } else {
            return Err(NoMetaDAtaError)
        }
        let other_unwrapped_iter = other_iterator.map(|x| {
            match x {
                RunnerDataBatch::Batch(data) => {data},
                RunnerDataBatch::MetaData(_) => {
                    // TODO get rid of panic
                    panic!("duplicate metadata error");
                }
            }
        });

        // TODO find a way to merge the metadatas
        Ok(Runner::new_with_meta(self_unwrapped_iter.zip(other_unwrapped_iter), self_meta_data))
    }
}

impl<'a, DataType: Sized, OpSignatureType: signatures::Signature + Clone> IntoIterator for Runner<'a, DataType, OpSignatureType> {
    type Item = RunnerDataBatch<DataType, OpSignatureType>;
    type IntoIter = RunnerIterator<'a, DataType, OpSignatureType>;

    fn into_iter(self) -> Self::IntoIter {
        RunnerIterator::new_form_box(self.meta_data, self.data)

    }
}
