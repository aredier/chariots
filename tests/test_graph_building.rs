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

struct AddTwoOp {
    signature: chariots::signatures::VersionedSignature
}

impl AddTwoOp {
    fn new () -> Self {
        AddTwoOp {signature: signatures::VersionedSignature::new(0, 1, 0)}
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
    signature: chariots::signatures::VersionedSignature
}

impl Sum {
    fn new () -> Self {
        Sum {signature: signatures::VersionedSignature::new(0, 1, 0)}
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
    let mut add2 = AddTwoOp::new();
    let mut sum = Sum::new();
    let left_runner  = runner1.chain(&mut add1).unwrap();
    let right_runner = runner2.chain(&mut add2).unwrap();
    let res_runner = left_runner.merge(right_runner).unwrap().chain(&mut sum).unwrap();

    // testing that the new data is as excpected
    let mut new_runner_iter = res_runner.into_iter();
    let meta_data_wraped = new_runner_iter.next();
    if let Some(runners::RunnerDataBatch::MetaData(meta_data)) = meta_data_wraped {
        // TODO add implement metadata merge and test here
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
