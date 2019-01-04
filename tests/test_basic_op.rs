extern crate chariots;
use chariots::*;
use chariots::ops::AbstractOp;

struct AddOneOp {
    signature: chariots::signatures::VersionedSignature
}

impl AddOneOp {
    fn new () -> Self {
        AddOneOp {signature: signatures::VersionedSignature::new(0, 1, 0)}
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

struct DevideByTwo {
    signature: chariots::signatures::VersionedSignature
}

impl DevideByTwo {
    fn new () -> Self {
        DevideByTwo {signature: signatures::VersionedSignature::new(0, 1, 0)}
    }
}

impl AbstractOp for DevideByTwo {
    type OpSignatureType = signatures::VersionedSignature;
    type InputDataType = usize;
    type OutputDataType = f32;

    fn signature(&self) -> Self::OpSignatureType {
        self.signature.clone()
    }

    fn  call (&self, data: Self::InputDataType) -> Self::OutputDataType {
        data as f32 / 2f32
    }

}

///test that we can create an op (that just adds one) and that we can get a subsequent runner
#[test]
fn test_basic_op_same_type() {
    let mut op = AddOneOp::new();
    let runner: runners::Runner<usize, signatures::VersionedSignature> = runners::Runner::new(vec!(0, 1, 2));
    let res_runner = runner.chain(&mut op).unwrap();

    // testing that the new data is as excpected
    let mut new_runner_iter = res_runner.into_iter();
    let meta_data_wraped = new_runner_iter.next();
    if let Some(runners::RunnerDataBatch::MetaData(meta_data)) = meta_data_wraped {
        let mut moc_metadata = runners::RunnerMetaData::new();
        moc_metadata.sign(signatures::VersionedSignature::new(0, 1, 0));
        assert_eq!(meta_data, moc_metadata);
    }
    else{
        panic!("the metadata is not the right type");
    }
    for (idx, batch) in new_runner_iter.enumerate() {
        // TODO this is ugly we should have a map at some point that de-intricates this datastructure
        if let runners::RunnerDataBatch::Batch(value) = batch {
            assert_eq!(idx + 1, value);
        }
    }
}


///test that changing the datatype of the op is supported
#[test]
fn test_basic_op_different_type() {
    let mut op = DevideByTwo::new();
    let runner: runners::Runner<usize, signatures::VersionedSignature> = runners::Runner::new(vec!(0, 1, 2));
    let res_runner = runner.chain(&mut op).unwrap();

    // testing that the new data is as excpected
    let mut new_runner_iter = res_runner.into_iter();
    let meta_data_wraped = new_runner_iter.next();
    if let Some(runners::RunnerDataBatch::MetaData(meta_data)) = meta_data_wraped {
        let mut moc_metadata = runners::RunnerMetaData::new();
        moc_metadata.sign(signatures::VersionedSignature::new(0, 1, 0));
        assert_eq!(meta_data, moc_metadata);
    }
    else{
        panic!("the metadata is not the right type");
    }
    for (idx, batch) in new_runner_iter.enumerate() {
        // TODO this is ugly we should have a map at some point that de-intricates this datastructure
        if let runners::RunnerDataBatch::Batch(value) = batch {
            assert_eq!(idx as f32 / 2f32, value);
        }
    }
}

/// test chaining ops
#[test]
fn test_chaining_ops() {
    let mut op = AddOneOp::new();
    let mut op2 = DevideByTwo::new();
    let runner: runners::Runner<usize, signatures::VersionedSignature> = runners::Runner::new(vec!(0, 1, 2));
    let res_runner = runner.chain(&mut op).unwrap().chain(&mut op2).unwrap();

    // testing that the new data is as excpected
    let mut new_runner_iter = res_runner.into_iter();
    let meta_data_wraped = new_runner_iter.next();
    if let Some(runners::RunnerDataBatch::MetaData(meta_data)) = meta_data_wraped {
        let mut moc_metadata = runners::RunnerMetaData::new();
        moc_metadata.sign(signatures::VersionedSignature::new(0, 1, 0));
        moc_metadata.sign(signatures::VersionedSignature::new(0, 1, 0));
        assert_eq!(meta_data, moc_metadata);
    }
    else{
        panic!("the metadata is not the right type");
    }
    for (idx, batch) in new_runner_iter.enumerate() {
        // TODO this is ugly we should have a map at some point that de-intricates this datastructure
        if let runners::RunnerDataBatch::Batch(value) = batch {
            assert_eq!((idx + 1) as f32 / 2f32, value);
        }
    }
}
