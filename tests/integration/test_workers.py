import subprocess
import time

import pytest
from flaky import flaky
from redis import Redis

from chariots import Chariots, Pipeline, TestClient, MLMode
from chariots.workers import JobStatus
from chariots.workers import RQWorkerPool
from chariots.errors import VersionError
from chariots.nodes import Node
from chariots._helpers.test_helpers import IsPair, WaitOp, XTrainOpL, PCAOp, YOp, SKLROp, LinearDataSet, KerasLogistic, \
    FromArray, ToArray, RQWorkerContext


def test_app_async(tmpdir):
    with RQWorkerContext():
        pipe1 = Pipeline([
            Node(IsPair(), input_nodes=['__pipeline_input__'], output_nodes='__pipeline_output__'),
        ], name="inner_pipe")

        app = Chariots([pipe1], path=str(tmpdir), import_name="some_app",
                       worker_pool=RQWorkerPool(redis=Redis()),
                       use_workers=True)
        test_client = TestClient(app)

        response = test_client.call_pipeline(pipe1, pipeline_input=list(range(20)))
        assert response.job_status == JobStatus.queued
        time.sleep(3)
        response = test_client.fetch_job(response.job_id, pipe1)
        assert response.job_status == JobStatus.done
        assert response.value == [not i % 2 for i in range(20)]


def test_app_async_pipeline(tmpdir):
    with RQWorkerContext():
        pipe1 = Pipeline([
            Node(IsPair(), input_nodes=["__pipeline_input__"], output_nodes="__pipeline_output__")
        ], name="inner_pipe", use_worker=True)

        app = Chariots([pipe1], path=str(tmpdir), import_name="some_app",
                       worker_pool=RQWorkerPool(redis=Redis()),
                       use_workers=False)
        test_client = TestClient(app)

        response = test_client.call_pipeline(pipe1, pipeline_input=list(range(20)))
        assert response.job_status == JobStatus.queued
        time.sleep(3)
        response = test_client.fetch_job(response.job_id, pipe1)
        assert response.job_status == JobStatus.done
        assert response.value == [not i % 2 for i in range(20)]


def test_app_async_request(tmpdir):
    with RQWorkerContext():
        pipe1 = Pipeline([
            Node(IsPair(), input_nodes=["__pipeline_input__"], output_nodes="__pipeline_output__")
        ], name="inner_pipe")

        app = Chariots([pipe1], path=str(tmpdir), import_name="some_app",
                       worker_pool=RQWorkerPool(redis=Redis()),
                       use_workers=False)
        test_client = TestClient(app)

        response = test_client.call_pipeline(pipe1, pipeline_input=list(range(20)), use_worker=True)
        assert response.job_status == JobStatus.queued
        time.sleep(3)
        response = test_client.fetch_job(response.job_id, pipe1)
        assert response.job_status == JobStatus.done
        assert response.value == [not i % 2 for i in range(20)]


def test_complex_sk_training_pipeline_async(tmpdir):
    with RQWorkerContext():
        train_transform = Pipeline([
            Node(XTrainOpL(), output_nodes="x_raw"),
            Node(PCAOp(mode=MLMode.FIT), input_nodes=["x_raw"], output_nodes="x_train"),
        ], "train_pca", use_worker=True)
        train_pipe = Pipeline([
            Node(XTrainOpL(), output_nodes="x_raw"),
            Node(YOp(), output_nodes="y_train"),
            Node(PCAOp(mode=MLMode.PREDICT), input_nodes=["x_raw"], output_nodes="x_train"),
            Node(SKLROp(mode=MLMode.FIT), input_nodes=["x_train", "y_train"])
        ], "train", use_worker=True)
        pred_pipe = Pipeline([
            Node(PCAOp(mode=MLMode.PREDICT), input_nodes=["__pipeline_input__"], output_nodes="x_train"),
            Node(SKLROp(mode=MLMode.PREDICT), input_nodes=["x_train"], output_nodes="__pipeline_output__")
        ], "pred")
        my_app = Chariots(app_pipelines=[train_transform, train_pipe, pred_pipe],
                          path=str(tmpdir), import_name="my_app", worker_pool=RQWorkerPool(redis=Redis()))

        test_client = TestClient(my_app)
        response = test_client.call_pipeline(train_transform)
        time.sleep(3.)
        response = test_client.fetch_job(response.job_id, train_transform)
        assert response.job_status == JobStatus.done
        # test_client.load_pipeline(train_pipe)
        response = test_client.call_pipeline(train_pipe)
        time.sleep(3.)
        response = test_client.fetch_job(response.job_id, train_pipe)
        assert response.job_status == JobStatus.done
        test_client.load_pipeline(pred_pipe)
        response = test_client.call_pipeline(pred_pipe, pipeline_input=[[100, 101, 102], [101, 102, 103], [102, 103, 104]])
        assert response.job_status == JobStatus.done

        assert len(response.value) == 3
        for i, individual_value in enumerate(response.value):
            assert abs(101 + i - individual_value) < 1e-5

        response = test_client.call_pipeline(train_transform)
        test_client.save_pipeline(train_transform)
        time.sleep(3.)
        response = test_client.fetch_job(response.job_id, train_transform)
        assert response.job_status == JobStatus.done
        with pytest.raises(VersionError):
            test_client.load_pipeline(pred_pipe)

@flaky(5, 1)
def test_train_keras_pipeline_async(tmpdir):

    with RQWorkerContext():
        train = Pipeline([
            Node(LinearDataSet(rows=10), output_nodes=['X', 'y']),
            Node(KerasLogistic(MLMode.FIT), input_nodes=['X', 'y'])
        ], 'train', use_worker=True)

        pred = Pipeline([
            Node(ToArray(output_shape=(-1, 1)), input_nodes=['__pipeline_input__'], output_nodes='X'),
            Node(KerasLogistic(MLMode.PREDICT), input_nodes=['X'], output_nodes='pred'),
            Node(FromArray(), input_nodes=['pred'], output_nodes='__pipeline_output__')
        ], 'pred')
        my_app = Chariots(app_pipelines=[train, pred], path=str(tmpdir), import_name="my_app",
                          worker_pool=RQWorkerPool(redis=Redis()))
        client = TestClient(my_app)
        client.call_pipeline(train)
        response = client.call_pipeline(train)
        time.sleep(10)
        response = client.fetch_job(response.job_id, train)
        assert response.job_status == JobStatus.done
        client.load_pipeline(pred)

        pred = client.call_pipeline(pred, [[5]]).value
        assert len(pred) == 1
        assert len(pred[0]) == 1
        assert 5 < pred[0][0] < 7
