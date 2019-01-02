/// module that provides wrappers around iterators in order to carry some metadata
/// (at th present time the signatures of the ops that ran it) along the data
use std::iter::FromIterator;

use super::signatures;

pub struct NoMetaDAtaError;


/// a simple iterator wrapper
pub struct IterWrapper<DataType: Sized, Iter: Iterator<Item=DataType>> {
    iterator: Iter,
}

impl <DataType: Sized, Iter: Iterator<Item=DataType>> IterWrapper<DataType, Iter> {
    pub fn new(iterator: Iter) -> Self {
        IterWrapper{iterator}
    }
}

impl <DataType: Sized, Iter: Iterator<Item=DataType>> IntoIterator for IterWrapper<DataType, Iter>{
    type Item=DataType;
    type IntoIter=Iter;

    fn into_iter(self) -> Self::IntoIter {
        self.iterator
    }
}


/// the actual runner that gets passed to the ops
pub struct Runner<DataType: Sized, IntoIter: IntoIterator<Item=DataType>, OpSignatureType: signatures::Signature> {
    meta_data: RunnerMetaData<DataType, OpSignatureType>,
    data: IntoIter,
}
impl<DataType: Sized, IntoIter: IntoIterator<Item=DataType>, OpSignatureType: signatures::Signature> Runner<DataType, IntoIter, OpSignatureType> {
    pub fn new(data: IntoIter) -> Self {
        Runner {data, meta_data: RunnerMetaData::new()}
    }

    pub fn new_with_meta(data:IntoIter, meta_data: RunnerMetaData) -> Self {
        Runner {data, meta_data}
    }
}

impl<DataType: Sized, IntoIter: IntoIterator<Item=DataType>, OpSignatureType: signatures::Signature> IntoIterator for Runner<DataType, IntoIter, OpSignatureType> {
    type Item = DataType;
    type IntoIter = RunnerIterator;

    fn into_iter(self) -> Self::IntoIter {
        RunnerIterator::new(self.meta_data, self.data.into_iter())
    }
}

impl<DataType: Sized,
    IntoIter: IntoIterator<Item=DataType>,
    OpSignatureType: signatures::Signature> FromIterator<DataType> for Runner<DataType, IntoIter, OpSignatureType> {

     fn from_iter(iter: IntoIter) -> Result<Self, NoMetaDAtaError> {
         let mut data_iterator = iter.into_iter();
         let meta_data;
         if let Some(RunnerDataBatch::MetaData(meta_data)) = data_iterator.next(){
             meta_data = meta_data;
         } else {
             return Err(NoMetaDAtaError)
         }
         Runner::new_with_meta(data_iterator, meta_data)
     }
 }


/// the runner's metatdata
struct RunnerMetaData<DataType: Sized, OpSignatureType: signatures::Signature> {
    signatures: Vec<OpSignatureType>,
}

impl<DataType: Sized, OpSignatureType: signatures::Signature> RunnerMetaData<DataType, OpSignatureType>{
    fn new() -> Self {
        RunnerMetaData {signatures: Vec::new()}
    }

    fn sign(&mut self, signature: OpSignatureType) {
        self.signatures.push(signature);
    }
}


/// a batch given to a runnerIterator if it's the first batch, than it is the meta data
/// otherwise it is the data of the batch itself
enum RunnerDataBatch<DataType: Sized, OpSignatureType: signatures::Signature> {
    MetaData(RunnerMetaData<DataType, OpSignatureType>),
    Batch(DataType),
}


/// a runner iterator is the iterator on which the Ops will map the call function
struct RunnerIterator<DataType: Sized, I: Iterator<Item = DataType>, OpSignatureType: signatures::Signature> {
    meta_data: RunnerMetaData<DataType, OpSignatureType>,
    data_iterator: I,
    is_first_element: bool,
}

impl<DataType: Sized, I: Iterator<Item = DataType>, OpSignatureType: signatures::Signature> RunnerIterator<DataType, I, OpSignatureType>{

    fn new(meta_data:  RunnerMetaData<DataType, OpSignatureType>, data_iterator: I) -> Self {
        RunnerIterator {meta_data, data_iterator, is_first_element: true}
    }
}

impl<DataType: Sized, I: Iterator<Item = DataType>, OpSignatureType: signatures::Signature> Iterator for RunnerIterator<DataType, I, OpSignatureType> {
    type Item = RunnerDataBatch<DataType, OpSignatureType>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_first_element {
            self.is_first_element = false;
            return Some(RunnerDataBatch::MetaData(self.meta_data));
        }
        match self.data_iterator.next() {
            Some(data) => {
                Some(RunnerDataBatch::Batch(data))
            },
            None => {None}
        }
    }
}
