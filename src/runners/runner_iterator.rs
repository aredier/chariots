use std::rc::Rc;

use super::*;

/// a runner iterator is the iterator on which the Ops will map the call function
pub struct RunnerIterator<'a, DataType: Sized, OpSignatureType: signatures::Signature + Clone> {
    meta_data: Rc<RunnerMetaData<OpSignatureType>>,
    data_iterator: Box<Iterator<Item=DataType> + 'a>,
    is_first_element: bool,
}

impl<'a, DataType: Sized, OpSignatureType: signatures::Signature + Clone> RunnerIterator<'a, DataType, OpSignatureType>{

    /// produces a new RunnerIterator
    pub fn new<I: Iterator<Item=DataType>>(meta_data:  Rc<RunnerMetaData<OpSignatureType>>, data_iterator: I) -> Self
    where I: 'a
    {
        RunnerIterator {meta_data,data_iterator: Box::new(data_iterator), is_first_element: true}
    }

    /// produces a new RunnerIterator from a Boxed Iterator
    pub fn new_form_box(meta_data:  Rc<RunnerMetaData<OpSignatureType>>, data_iterator: Box<Iterator<Item=DataType> + 'a>) -> Self {
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
