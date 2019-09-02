import pytest
import numpy as np
from sklearn.decomposition import PCA
from sklearn.linear_model import LinearRegression

from chariots import MLMode, Pipeline, Chariots, TestClient
from chariots.base import BaseMLOp, BaseOp
from chariots.errors import VersionError
from chariots.nodes import Node
from chariots.sklearn import SKUnsupervisedOp, SKSupervisedOp
from chariots.versioning import VersionType, VersionedField, VersionedFieldDict


@pytest.fixture
def LROp():
    class LROpInner(BaseMLOp):

        def fit(self, x_train, y_train):
            self._model.fit(np.array(x_train).reshape(-1, 1), y_train)

        def predict(self, x_pred):
            if not isinstance(x_pred, list):
                x_pred = [x_pred]
            return int(self._model.predict(np.array(x_pred).reshape(-1, 1))[0])

        def _init_model(self):
            return LinearRegression()
    return LROpInner


def test_raw_training_pipeline(LROp, YOp, XTrainOp, tmpdir):
    train_pipe = Pipeline([
        Node(XTrainOp(), output_nodes="x_train"),
        Node(YOp(), output_nodes="y_train"),
        Node(LROp(mode=MLMode.FIT), input_nodes=["x_train", "y_train"])
    ], "train")
    pred_pipe = Pipeline([
        Node(LROp(mode=MLMode.PREDICT), input_nodes=["__pipeline_input__"],
                                  output_nodes="__pipeline_output__")
    ], "pred")
    my_app = Chariots(app_pipelines=[train_pipe, pred_pipe], path=str(tmpdir), import_name="my_app")

    test_client = TestClient(my_app)
    prior_versions_train = test_client.pipeline_versions(train_pipe)
    prior_versions_pred = test_client.pipeline_versions(pred_pipe)
    test_client._request(train_pipe)
    test_client.save_pipeline(train_pipe)
    test_client.load_pipeline(pred_pipe)
    posterior_versions_train = test_client.pipeline_versions(train_pipe)
    posterior_versions_pred = test_client.pipeline_versions(pred_pipe)
    response = test_client._request(pred_pipe, pipeline_input=3)

    assert response.value == 4

    # testing verison uodate
    lrop_train = train_pipe.node_for_name["lropinner"]
    lrop_pred = pred_pipe.node_for_name["lropinner"]
    assert prior_versions_train[lrop_train.name] == prior_versions_pred[lrop_pred.name]
    assert posterior_versions_train[lrop_train.name] == posterior_versions_pred[lrop_pred.name]
    assert prior_versions_pred[lrop_pred.name] < posterior_versions_pred[lrop_pred.name]


@pytest.fixture
def SKLROp():
    class SKLROpInner(SKSupervisedOp):
        model_class = VersionedField(LinearRegression, VersionType.MINOR)

    return SKLROpInner


def test_sk_training_pipeline(SKLROp, YOp, XTrainOp, tmpdir):
    train_pipe = Pipeline([
        Node(XTrainOp(), output_nodes="x_train"),
        Node(YOp(), output_nodes="y_train"),
        Node(SKLROp(mode=MLMode.FIT), input_nodes=["x_train", "y_train"])
    ], "train")
    pred_pipe = Pipeline([
        Node(SKLROp(mode=MLMode.PREDICT), input_nodes=["__pipeline_input__"],
                                  output_nodes="__pipeline_output__")
    ], "pred")
    my_app = Chariots(app_pipelines=[train_pipe, pred_pipe], path=str(tmpdir), import_name="my_app")

    test_client = TestClient(my_app)
    prior_versions_train = test_client.pipeline_versions(train_pipe)
    prior_versions_pred = test_client.pipeline_versions(pred_pipe)
    test_client._request(train_pipe)
    test_client.save_pipeline(train_pipe)
    test_client.load_pipeline(pred_pipe)
    posterior_versions_train = test_client.pipeline_versions(train_pipe)
    posterior_versions_pred = test_client.pipeline_versions(pred_pipe)
    response = test_client._request(pred_pipe, pipeline_input=[[100], [101], [102]])

    assert len(response.value) == 3
    for i, individual_value in enumerate(response.value):
        assert abs(101 + i - individual_value) < 1e-5

    # testing version update
    lrop_train = train_pipe.node_for_name["sklropinner"]
    lrop_pred = pred_pipe.node_for_name["sklropinner"]
    assert prior_versions_train[lrop_train.name]== prior_versions_pred[lrop_pred.name]
    assert posterior_versions_train[lrop_train.name] == posterior_versions_pred[lrop_pred.name]
    assert prior_versions_pred[lrop_pred.name] < posterior_versions_pred[lrop_pred.name]


@pytest.fixture
def PCAOp():
    class PCAInner(SKUnsupervisedOp):
        training_update_version = VersionType.MAJOR
        model_parameters =VersionedFieldDict(
            VersionType.MAJOR, {
            "n_components": 2,
        })
        model_class = VersionedField(PCA, VersionType.MAJOR)

    return PCAInner


@pytest.fixture
def XTrainOpL():
    class XTrainOpInner(BaseOp):

        def execute(self):
            return np.array([range(10), range(1, 11), range(2, 12)]).T
    return XTrainOpInner


def test_complex_sk_training_pipeline(SKLROp, YOp, XTrainOpL, PCAOp, tmpdir):

    train_transform = Pipeline([
        Node(XTrainOpL(), output_nodes="x_raw"),
        Node(PCAOp(mode=MLMode.FIT), input_nodes=["x_raw"], output_nodes="x_train"),
    ], "train_pca")
    train_pipe = Pipeline([
        Node(XTrainOpL(), output_nodes="x_raw"),
        Node(YOp(), output_nodes="y_train"),
        Node(PCAOp(mode=MLMode.PREDICT), input_nodes=["x_raw"], output_nodes="x_train"),
        Node(SKLROp(mode=MLMode.FIT), input_nodes=["x_train", "y_train"])
    ], "train")
    pred_pipe = Pipeline([
        Node(PCAOp(mode=MLMode.PREDICT), input_nodes=["__pipeline_input__"], output_nodes="x_train"),
        Node(SKLROp(mode=MLMode.PREDICT), input_nodes=["x_train"], output_nodes="__pipeline_output__")
    ], "pred")
    my_app = Chariots(app_pipelines=[train_transform, train_pipe, pred_pipe],
                      path=str(tmpdir), import_name="my_app")

    test_client = TestClient(my_app)
    test_client._request(train_transform)
    test_client.save_pipeline(train_transform)
    test_client.load_pipeline(train_pipe)
    test_client._request(train_pipe)
    test_client.save_pipeline(train_pipe)
    test_client.load_pipeline(pred_pipe)
    response = test_client._request(pred_pipe, pipeline_input=[[100, 101, 102], [101, 102, 103], [102, 103, 104]])

    assert len(response.value) == 3
    for i, individual_value in enumerate(response.value):
        assert abs(101 + i - individual_value) < 1e-5

    test_client._request(train_transform)
    test_client.save_pipeline(train_transform)
    with pytest.raises(VersionError):
        test_client.load_pipeline(pred_pipe)


def test_fit_predict_pipeline_reload(SKLROp, YOp, XTrainOpL, PCAOp, tmpdir):

    train_pca = Pipeline([
        Node(XTrainOpL(), output_nodes="x_raw"),
        Node(PCAOp(mode=MLMode.FIT), input_nodes=["x_raw"]),
        ], "train_pca")
    train_rf = Pipeline([
        Node(XTrainOpL(), output_nodes="x_raw"),
        Node(YOp(), output_nodes="y_train"),
        Node(PCAOp(mode=MLMode.PREDICT), input_nodes=["x_raw"], output_nodes="x_train"),
        Node(SKLROp(mode=MLMode.FIT), input_nodes=["x_train", "y_train"])
    ], "train_rf")
    pred_pipe = Pipeline([
        Node(PCAOp(mode=MLMode.PREDICT), input_nodes=["__pipeline_input__"], output_nodes="x_train"),
        Node(SKLROp(mode=MLMode.PREDICT), input_nodes=["x_train"], output_nodes="__pipeline_output__")
    ], "pred")
    my_app = Chariots(app_pipelines=[train_pca, train_rf, pred_pipe],
                      path=str(tmpdir), import_name="my_app")

    test_client = TestClient(my_app)

    # test that the train save load is working
    test_client._request(train_pca)
    test_client.save_pipeline(train_pca)
    test_client.load_pipeline(train_rf)
    test_client._request(train_rf)
    test_client.save_pipeline(train_rf)
    test_client.load_pipeline(pred_pipe)
    response = test_client._request(pred_pipe, pipeline_input=[[100, 101, 102], [101, 102, 103], [102, 103, 104]])

    assert len(response.value) == 3
    for i, individual_value in enumerate(response.value):
        assert abs(101 + i - individual_value) < 1e-5

    # test that that retrain is possible
    test_client._request(train_pca)
    test_client.save_pipeline(train_pca)
    test_client.load_pipeline(train_rf)
    test_client._request(train_rf)
    test_client.save_pipeline(train_rf)
    test_client.load_pipeline(pred_pipe)
    response = test_client._request(pred_pipe, pipeline_input=[[100, 101, 102], [101, 102, 103], [102, 103, 104]])

    assert len(response.value) == 3
    for i, individual_value in enumerate(response.value):
        assert abs(101 + i - individual_value) < 1e-5

    # test that that wrong loading is raising
    test_client._request(train_pca)
    test_client.save_pipeline(train_pca)
    with pytest.raises(VersionError):
        test_client.load_pipeline(pred_pipe)
    response = test_client._request(pred_pipe, pipeline_input=[[100, 101, 102], [101, 102, 103], [102, 103, 104]])

    assert len(response.value) == 3
    for i, individual_value in enumerate(response.value):
        assert abs(101 + i - individual_value) < 1e-5

