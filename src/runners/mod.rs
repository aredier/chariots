/// module that provides wrappers around iterators in order to carry some metadata
/// (at th present time the signatures of the ops that ran it) along the data
use std::clone::Clone;

use super::signatures;

pub struct NoMetaDAtaError;


/// the actual runner that gets passed to the ops
pub struct Runner<'a, DataType: Sized, OpSignatureType: signatures::Signature + Clone> {
    meta_data: RunnerMetaData<OpSignatureType>,
    data: Box<Iterator<Item=DataType> + 'a>,
}

impl<'a, DataType: Sized, OpSignatureType: signatures::Signature + Clone> Runner<'a, DataType, OpSignatureType> {

    pub fn new<'b: 'a, IntoIter: IntoIterator<Item=DataType>> (data: IntoIter) -> Self
    where IntoIter::IntoIter: 'b {
        Runner {data: Box::new(data.into_iter()), meta_data: RunnerMetaData::new()}
    }

    pub fn sign (&mut self, signature: OpSignatureType) {
        self.meta_data.sign(signature);
    }

    fn new_with_meta<'b: 'a, IntoIter: IntoIterator<Item=DataType>> (data: IntoIter, meta_data: RunnerMetaData<OpSignatureType>) -> Self
    where IntoIter::IntoIter: 'b {
        Runner {data: Box::new(data.into_iter()), meta_data}
    }

    pub fn from_runner_iterator<'b, Iter: Iterator<Item=RunnerDataBatch<DataType, OpSignatureType>>> (mut iter: Iter, )
    ->  Result<Runner<'b, RunnerDataBatch<DataType, OpSignatureType>, OpSignatureType>, NoMetaDAtaError>
    where Iter: 'b
     {
        let meta_data: RunnerMetaData<OpSignatureType>;
        if let Some(RunnerDataBatch::MetaData(meta)) = iter.next(){
            meta_data = meta;
        } else {
            return Err(NoMetaDAtaError)
        }
        Ok(Runner::new_with_meta(Box::new(iter), meta_data))
    }
}

impl<'a, DataType: Sized, OpSignatureType: signatures::Signature + Clone> IntoIterator for Runner<'a, DataType, OpSignatureType> {
    type Item = RunnerDataBatch<DataType, OpSignatureType>;
    type IntoIter = RunnerIterator<'a, DataType, OpSignatureType>;

    fn into_iter(self) -> Self::IntoIter {
        RunnerIterator::new_form_box(self.meta_data, self.data)

    }
}


/// the runner's metatdata
#[derive(Clone)]
pub struct RunnerMetaData<OpSignatureType: signatures::Signature + Clone> {
    signatures: Vec<OpSignatureType>,
}

impl<OpSignatureType: signatures::Signature + Clone> RunnerMetaData<OpSignatureType>{
    fn new() -> Self {
        RunnerMetaData {signatures: Vec::new()}
    }

    pub fn sign(&mut self, signature: OpSignatureType) {
        self.signatures.push(signature);
    }
}


/// a batch given to a runnerIterator if it's the first batch, than it is the meta data
/// otherwise it is the data of the batch itself
pub enum RunnerDataBatch<DataType: Sized, OpSignatureType: signatures::Signature + Clone> {
    MetaData(RunnerMetaData<OpSignatureType>),
    Batch(DataType),
}


/// a runner iterator is the iterator on which the Ops will map the call function
pub struct RunnerIterator<'a, DataType: Sized, OpSignatureType: signatures::Signature + Clone> {
    meta_data: RunnerMetaData<OpSignatureType>,
    data_iterator: Box<Iterator<Item=DataType> + 'a>,
    is_first_element: bool,
}

impl<'a, DataType: Sized, OpSignatureType: signatures::Signature + Clone> RunnerIterator<'a, DataType, OpSignatureType>{

    pub fn new<I: Iterator<Item=DataType>>(meta_data:  RunnerMetaData<OpSignatureType>, data_iterator: I) -> Self
    where I: 'a
    {
        RunnerIterator {meta_data,data_iterator: Box::new(data_iterator), is_first_element: true}
    }

    pub fn new_form_box(meta_data:  RunnerMetaData<OpSignatureType>, data_iterator: Box<Iterator<Item=DataType> + 'a>) -> Self {
        RunnerIterator {meta_data, data_iterator, is_first_element: true}
    }
}

impl<'a, DataType: Sized, OpSignatureType: signatures::Signature + Clone> Iterator for RunnerIterator<'a, DataType, OpSignatureType> {
    type Item = RunnerDataBatch<DataType, OpSignatureType>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_first_element {
            self.is_first_element = false;
            return Some(RunnerDataBatch::MetaData(self.meta_data.clone()));
        }
        match (*self.data_iterator).next() {
            Some(data) => {
                Some(RunnerDataBatch::Batch(data))
            },
            None => {None}
        }
    }
}
