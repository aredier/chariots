#[cfg(test)]

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
    let new_runner: Runner<usize, signatures::VersionedSignature> = Runner::from_runner_iterator(mapped_runner).unwrap();
    let mut new_runner_iter = new_runner.into_iter();
    let meta_data_wraped: Option<RunnerDataBatch<usize, signatures::VersionedSignature>> = new_runner_iter.next();
    if let Some(RunnerDataBatch::MetaData(meta_data)) = meta_data_wraped {
        assert_eq!(meta_data, RunnerMetaData::new());
    }
    else{
        panic!("the metadata is not the right type");
    }
    for (idx, batch) in new_runner_iter.enumerate() {
        // TODO this is ugly we should have a map at some point that de-intricates this datastructure (see #3)
        if let RunnerDataBatch::Batch(value) = batch {
            assert_eq!(idx + 1, value);
        }
    }
}
