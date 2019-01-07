use super::*;
use std::rc::Rc;

pub type Link<T> = Option<Rc<T>>;

/// the runner's metatdata
#[derive(Clone, PartialEq, Debug, Default)]
pub struct RunnerMetaData<OpSignatureType: signatures::Signature + Clone>
{
    previous_meta_datas: Vec<Link<RunnerMetaData<OpSignatureType>>>,
    last_op_signature: Option<OpSignatureType>,
}

impl<OpSignatureType: signatures::Signature + Clone> RunnerMetaData<OpSignatureType>{

    /// produces a new MetaData
    pub fn new() -> Rc<Self> {
        Rc::new(RunnerMetaData {previous_meta_datas: Vec::new(), last_op_signature: None})
    }

    pub fn sign(metadata_reference: &Rc<Self>, signature: OpSignatureType) -> Rc<RunnerMetaData<OpSignatureType>>
    {
        Rc::new(RunnerMetaData {
            previous_meta_datas: vec!(Some(Rc::clone(&metadata_reference))),
            last_op_signature: Some(signature),
        })
    }
}
