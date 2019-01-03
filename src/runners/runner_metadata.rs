use super::*;


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
