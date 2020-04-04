"""
this is a test module testing the workers api of Chariots
"""
import time

import pytest
from flaky import flaky
from redis import Redis

from chariots import Chariots, Pipeline, TestClient
from chariots.workers import JobStatus
from chariots.workers import RQWorkerPool
from chariots.errors import VersionError
from chariots.nodes import Node
from chariots._helpers.test_helpers import IsPair, RQWorkerContext, build_keras_pipeline, \
    do_keras_pipeline_predictions_test


def do_async_pipeline_test(test_client, pipe, use_worker=None):
    """helper function that tests the basic pipeline with just the IsPair op"""
    response = test_client.call_pipeline(pipe, pipeline_input=list(range(20)), use_worker=use_worker)
    assert response.job_status == JobStatus.queued
    time.sleep(5)
    response = test_client.fetch_job(response.job_id, pipe)
    assert response.job_status == JobStatus.done
    assert response.value == [not i % 2 for i in range(20)]


def test_app_async(tmpdir, opstore_func):
    """tests executing the app async with the app setting `use_workers` to true"""
    with RQWorkerContext():
        pipe1 = Pipeline([
            Node(IsPair(), input_nodes=['__pipeline_input__'], output_nodes='__pipeline_output__'),
        ], name='inner_pipe')

        app = Chariots([pipe1], op_store_client=opstore_func(tmpdir), import_name='some_app',
                       worker_pool=RQWorkerPool(redis=Redis()),
                       use_workers=True)
        test_client = TestClient(app)
        do_async_pipeline_test(test_client, pipe1)


def test_app_async_pipeline(tmpdir, opstore_func):
    """tests executing the app async with the pipeline setting `use_workers` to true"""
    with RQWorkerContext():
        pipe1 = Pipeline([
            Node(IsPair(), input_nodes=['__pipeline_input__'], output_nodes='__pipeline_output__')
        ], name='inner_pipe', use_worker=True)

        app = Chariots([pipe1], op_store_client=opstore_func(tmpdir), import_name='some_app',
                       worker_pool=RQWorkerPool(redis=Redis()))
        test_client = TestClient(app)

        do_async_pipeline_test(test_client, pipe1)


def test_app_async_request(tmpdir, opstore_func):
    """tests executing the app async with the client setting `use_workers` to true"""
    with RQWorkerContext():
        pipe1 = Pipeline([
            Node(IsPair(), input_nodes=['__pipeline_input__'], output_nodes='__pipeline_output__')
        ], name='inner_pipe')

        app = Chariots([pipe1], op_store_client=opstore_func(tmpdir), import_name='some_app',
                       worker_pool=RQWorkerPool(redis=Redis()))
        test_client = TestClient(app)

        do_async_pipeline_test(test_client, pipe1, use_worker=True)


def test_app_async_conflicting_config(tmpdir, opstore_func):
    """
    tests the behavior when their are conflicts in the `use_workers` config (there is at least one True and one False)
    """
    with RQWorkerContext():
        pipe1 = Pipeline([
            Node(IsPair(), input_nodes=['__pipeline_input__'], output_nodes='__pipeline_output__')
        ], name='inner_pipe', use_worker=True)

        app = Chariots([pipe1], op_store_client=opstore_func(tmpdir), import_name='some_app',
                       worker_pool=RQWorkerPool(redis=Redis()), use_workers=False)
        test_client = TestClient(app)

        response = test_client.call_pipeline(pipe1, pipeline_input=list(range(20)), use_worker=True)
        assert response.job_status == JobStatus.done
        assert response.value == [not i % 2 for i in range(20)]


# this needs to be flaky because it might take a little bit longer
@flaky(3, 1)
def test_complex_sk_training_pipeline_async(complex_sk_pipelines, tmpdir, opstore_func):
    """tests the async with a more complexe sklearn based pipeline"""
    with RQWorkerContext():
        train_transform, train_pipe, pred_pipe = complex_sk_pipelines
        train_transform.use_worker = True
        train_pipe.use_worker = True

        my_app = Chariots(app_pipelines=[train_transform, train_pipe, pred_pipe],
                          op_store_client=opstore_func(tmpdir), import_name='my_app', worker_pool=RQWorkerPool(redis=Redis()))

        test_client = TestClient(my_app)
        response = test_client.call_pipeline(train_transform)
        time.sleep(5.)
        response = test_client.fetch_job(response.job_id, train_transform)
        assert response.job_status == JobStatus.done
        # test_client.load_pipeline(train_pipe)
        response = test_client.call_pipeline(train_pipe)
        time.sleep(5.)
        response = test_client.fetch_job(response.job_id, train_pipe)
        assert response.job_status == JobStatus.done
        test_client.load_pipeline(pred_pipe)
        response = test_client.call_pipeline(pred_pipe, pipeline_input=[
            [100, 101, 102],
            [101, 102, 103],
            [102, 103, 104]])
        assert response.job_status == JobStatus.done

        assert len(response.value) == 3
        for i, individual_value in enumerate(response.value):
            assert abs(101 + i - individual_value) < 1e-5

        response = test_client.call_pipeline(train_transform)
        test_client.save_pipeline(train_transform)
        time.sleep(5.)
        response = test_client.fetch_job(response.job_id, train_transform)
        assert response.job_status == JobStatus.done
        with pytest.raises(VersionError):
            test_client.load_pipeline(pred_pipe)


@flaky(5, 1)
def test_train_keras_pipeline_async(tmpdir, opstore_func):
    """test the workers with a keras pipeline"""
    with RQWorkerContext():
        train_pipeline, pred_pipeline = build_keras_pipeline(train_async=True)
        my_app = Chariots(app_pipelines=[train_pipeline, pred_pipeline], op_store_client=opstore_func(tmpdir),
                          import_name='my_app', worker_pool=RQWorkerPool(redis=Redis()))
        client = TestClient(my_app)
        client.call_pipeline(train_pipeline)
        response = client.call_pipeline(train_pipeline)
        time.sleep(10)
        response = client.fetch_job(response.job_id, train_pipeline)
        assert response.job_status == JobStatus.done
        client.load_pipeline(pred_pipeline)

        do_keras_pipeline_predictions_test(pred_pipeline, client)
