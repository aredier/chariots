extern crate chariots;

use std::rc::Rc;

use chariots::*;
use chariots::ops::AbstractOp;

struct AddOneOp {
    name: String,
    signature: chariots::signatures::VersionedSignature
}

impl AddOneOp {
    fn new () -> Self {
        let name = "add1".to_string();
        AddOneOp {name: name.clone(), signature: signatures::VersionedSignature::new(name.clone(), 0, 1, 0)}
    }
}

impl AbstractOp for AddOneOp {
    type OpSignatureType = signatures::VersionedSignature;
    type InputDataType = usize;
    type OutputDataType = usize;

    fn signature(&self) -> Self::OpSignatureType {
        self.signature.clone()
    }

    fn  call (&self, data: Self::InputDataType) -> Self::OutputDataType {
        data + 1
    }

}

struct AddTwoOp {
    name: String,
    signature: chariots::signatures::VersionedSignature
}

impl AddTwoOp {
    fn new () -> Self {
        let name = "add2".to_string();
        AddTwoOp {name: name.clone(), signature: signatures::VersionedSignature::new(name.clone(), 0, 1, 0)}
    }
}

impl AbstractOp for AddTwoOp {
    type OpSignatureType = signatures::VersionedSignature;
    type InputDataType = usize;
    type OutputDataType = usize;

    fn signature(&self) -> Self::OpSignatureType {
        self.signature.clone()
    }

    fn  call (&self, data: Self::InputDataType) -> Self::OutputDataType {
        data + 2
    }

}

struct Sum {
    name: String,
    signature: chariots::signatures::VersionedSignature
}

impl Sum {
    fn new () -> Self {
        let name = "sum".to_string();
        Sum {name: name.clone(), signature: signatures::VersionedSignature::new(name.clone(), 0, 1, 0)}
    }
}

impl AbstractOp for Sum {
    type OpSignatureType = signatures::VersionedSignature;
    type InputDataType = (usize, usize);
    type OutputDataType = usize;

    fn signature(&self) -> Self::OpSignatureType {
        self.signature.clone()
    }

    fn  call (&self, data: Self::InputDataType) -> Self::OutputDataType {
        data.0 + data.1
    }

}


/// tests we can merge two separate graphs
#[test]
fn test_merge() {
    let runner1: runners::Runner<usize, signatures::VersionedSignature> = runners::Runner::new(vec!(0, 1, 2, 3));
    let runner2: runners::Runner<usize, signatures::VersionedSignature> = runners::Runner::new(vec!(0, 1, 2, 3));
    let mut add1 = AddOneOp::new();
    let name1 = add1.name.clone();
    let mut add2 = AddTwoOp::new();
    let name2 = add2.name.clone();
    let mut sum = Sum::new();
    let name3 = sum.name.clone();
    let left_runner  = runner1.chain(&mut add1).unwrap();
    let right_runner = runner2.chain(&mut add2).unwrap();
    let res_runner = left_runner.merge(right_runner).unwrap().chain(&mut sum).unwrap();

    // testing that the new data is as excpected
    let mut new_runner_iter = res_runner.into_iter();
    let meta_data_wraped = new_runner_iter.next();
    if let Some(runners::RunnerDataBatch::MetaData(meta_data)) = meta_data_wraped {
        let mut moc_metadata1 = runners::RunnerMetaData::new();
        moc_metadata1 = runners::RunnerMetaData::sign(&moc_metadata1, signatures::VersionedSignature::new(name1, 0, 1, 0));
        let mut moc_metadata2 = runners::RunnerMetaData::new();
        moc_metadata2 = runners::RunnerMetaData::sign(&moc_metadata2, signatures::VersionedSignature::new(name2, 0, 1, 0));
        let mut moc_metadata = runners::RunnerMetaData::merge(moc_metadata1, moc_metadata2);
        moc_metadata = runners::RunnerMetaData::sign(&moc_metadata, signatures::VersionedSignature::new(name3, 0, 1, 0));
        assert_eq!(moc_metadata, meta_data);
    }
    else{
        panic!("the metadata is not the right type");
    }
    for (idx, batch) in new_runner_iter.enumerate() {
        if let runners::RunnerDataBatch::Batch(value) = batch {
            assert_eq!((idx * 2) + 3, value);
        }
    }

}
