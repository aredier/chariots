/// module that provides wrappers around iterators in order to carry some metadata
/// (at th present time the signatures of the ops that ran it) along the data
use std::clone::Clone;

use super::signatures;

#[derive(Debug)]
pub struct NoMetaDAtaError;


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
    ->  Result<Runner<'b, RunnerDataBatch<DataType, OpSignatureType>, OpSignatureType>, NoMetaDAtaError>
    where Iter: 'b
     {
        let meta_data: RunnerMetaData<OpSignatureType>;
        if let Some(RunnerDataBatch::MetaData(meta)) = iter.next(){
            meta_data = meta;
        } else {
            return Err(NoMetaDAtaError)
        }
        Ok(Runner::new_with_meta(iter, meta_data))
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
#[derive(Clone, PartialEq, Debug, Default)]
pub struct RunnerMetaData<OpSignatureType: signatures::Signature + Clone> {
    signatures: Vec<OpSignatureType>,
}

impl<OpSignatureType: signatures::Signature + Clone> RunnerMetaData<OpSignatureType>{

    /// produces a new MetaData
    pub fn new() -> Self {
        RunnerMetaData {signatures: Vec::new()}
    }

    pub fn sign(&mut self, signature: OpSignatureType) {
        self.signatures.push(signature);
    }
}


/// a batch given to a runnerIterator if it's the first batch, than it is the meta data
/// otherwise it is the data of the batch itself
#[derive(Debug)]
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

    /// produces a new RunnerIterator
    pub fn new<I: Iterator<Item=DataType>>(meta_data:  RunnerMetaData<OpSignatureType>, data_iterator: I) -> Self
    where I: 'a
    {
        RunnerIterator {meta_data,data_iterator: Box::new(data_iterator), is_first_element: true}
    }

    /// produces a new RunnerIterator from a Boxed Iterator
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



#[cfg(test)]
mod tests {
    use super::*;

    /// tests that a Runenr can be iterated over
    #[test]
    fn test_runner_iteration() {
        let runner: Runner<usize, signatures::VersionedSignature> = Runner::new(vec!(0, 1, 2));
        let mut runner_iter = runner.into_iter();
        let meta_data_wraped = runner_iter.next();
        if let Some(RunnerDataBatch::MetaData(meta_data)) = meta_data_wraped {
            assert_eq!(meta_data, RunnerMetaData::new());
        }else{
            panic!("the metadata is not the right type");
        }
        for (idx, batch) in runner_iter.enumerate() {
            if let RunnerDataBatch::Batch(value) = batch {
                assert_eq!(idx, value);
            }
        }
    }

    /// tests that a Runner can be mapped and cast back into an other Runner (with the right values)
    #[test]
    fn test_runner_map_conversion() {
        let runner: Runner<usize, signatures::VersionedSignature> = Runner::new(vec!(0, 1, 2));
        let runner_iter = runner.into_iter();
        let mapped_runner = runner_iter.map(|x| {
            match x {
                RunnerDataBatch::MetaData(meta_data) => {
                    RunnerDataBatch::MetaData(meta_data)
                },
                RunnerDataBatch::Batch(data) => {
                    RunnerDataBatch::Batch(data + 1)
                }
            }
        });

        // testing that the new data is as excpected
        let new_runner: Runner<RunnerDataBatch<usize, signatures::VersionedSignature>, signatures::VersionedSignature> = Runner::from_runner_iterator(mapped_runner).unwrap();
        let mut new_runner_iter = new_runner.into_iter();
    let meta_data_wraped: Option<RunnerDataBatch<RunnerDataBatch<usize, signatures::VersionedSignature>> = new_runner_iter.next();
        if let Some(RunnerDataBatch::MetaData(meta_data)) = meta_data_wraped {
            assert_eq!(meta_data, RunnerMetaData::new());
        }
        else{
            panic!("the metadata is not the right type");
        }
        for (idx, batch) in new_runner_iter.enumerate() {
            // TODO this is ugly we should have a map at some point that de-intricates this datastructure (see #3)
            if let RunnerDataBatch::Batch(value)) = batch {
                assert_eq!(idx + 1, value);
            }
        }
    }
}
