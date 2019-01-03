use super::*;

/// the actual runner that gets passed to the ops
pub struct Runner<'a, DataType: Sized, OpSignatureType: signatures::Signature + Clone> {
    meta_data: RunnerMetaData<OpSignatureType>,
    data: Box<Iterator<Item=DataType> + 'a>,
}

impl<'a, DataType: Sized, OpSignatureType: signatures::Signature + Clone> Runner<'a, DataType, OpSignatureType> {

    /// produces a new runner with blank metadata
    pub fn new<'b: 'a, IntoIter: IntoIterator<Item=DataType>> (data: IntoIter) -> Self
    where IntoIter::IntoIter: 'b {
        Runner {data: Box::new(data.into_iter()), meta_data: RunnerMetaData::new()}
    }

    /// signs the runner with some OpSignature
    pub fn sign (&mut self, signature: OpSignatureType) {
        self.meta_data.sign(signature);
    }

    /// creates a new runner from an existing metadata
    fn new_with_meta<'b: 'a, IntoIter: IntoIterator<Item=DataType>> (data: IntoIter, meta_data: RunnerMetaData<OpSignatureType>) -> Self
    where IntoIter::IntoIter: 'b {
        Runner {data: Box::new(data.into_iter()), meta_data}
    }

    /// creates a new Runner from a DataTypeor that has a metadata as it's first element
    pub fn from_runner_iterator<'b, Iter: Iterator<Item=RunnerDataBatch<DataType, OpSignatureType>>> (mut iter: Iter, )
    ->  Result<Runner<'b, DataType, OpSignatureType>, NoMetaDAtaError>
    where Iter: 'b
     {
        let meta_data: RunnerMetaData<OpSignatureType>;
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
}

impl<'a, DataType: Sized, OpSignatureType: signatures::Signature + Clone> IntoIterator for Runner<'a, DataType, OpSignatureType> {
    type Item = RunnerDataBatch<DataType, OpSignatureType>;
    type IntoIter = RunnerIterator<'a, DataType, OpSignatureType>;

    fn into_iter(self) -> Self::IntoIter {
        RunnerIterator::new_form_box(self.meta_data, self.data)

    }
}
