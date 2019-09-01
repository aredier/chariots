import pytest
import numpy as np
from sklearn.decomposition import PCA
from sklearn.linear_model import LinearRegression

import chariots._ml_mode
import chariots._pipeline
import chariots.base._base_ml_op
import chariots.nodes._node
import chariots.sklearn._sk_supervised_op
import chariots.sklearn._sk_unsupervised_op
import chariots.versioning
import chariots.versioning._version_type
import chariots.versioning._versioned_field
import chariots.versioning._versioned_field_dict
from chariots._deployment import app
from chariots._deployment.client import TestClient
from chariots.base._base_op import BaseOp
from chariots.errors import VersionError
from chariots import MLMode, Pipeline
from chariots.base import BaseMLOp
from chariots.sklearn import SKSupervisedOp,SKUnsupervisedOp

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
        chariots.nodes._node.Node(XTrainOp(), output_nodes="x_train"),
        chariots.nodes._node.Node(YOp(), output_nodes="y_train"),
        chariots.nodes._node.Node(LROp(mode=MLMode.FIT), input_nodes=["x_train", "y_train"])
    ], "train")
    pred_pipe = Pipeline([
        chariots.nodes._node.Node(LROp(mode=MLMode.PREDICT), input_nodes=["__pipeline_input__"],
                                  output_nodes="__pipeline_output__")
    ], "pred")
    my_app = app.Chariot(app_pipelines=[train_pipe, pred_pipe], path=str(tmpdir), import_name="my_app")

    test_client = TestClient(my_app)
    prior_versions_train = test_client.pipeline_versions(train_pipe)
    prior_versions_pred = test_client.pipeline_versions(pred_pipe)
    test_client.request(train_pipe)
    test_client.save_pipeline(train_pipe)
    test_client.load_pipeline(pred_pipe)
    posterior_versions_train = test_client.pipeline_versions(train_pipe)
    posterior_versions_pred = test_client.pipeline_versions(pred_pipe)
    response = test_client.request(pred_pipe, pipeline_input=3)

    assert response.value == 4

    # testing verison uodate
    lrop_train = train_pipe.node_for_name["lropinner"]
    lrop_pred = pred_pipe.node_for_name["lropinner"]
    assert prior_versions_train[lrop_train] == prior_versions_pred[lrop_pred]
    assert posterior_versions_train[lrop_train] == posterior_versions_pred[lrop_pred]
    assert prior_versions_pred[lrop_pred] < posterior_versions_pred[lrop_pred]


@pytest.fixture
def SKLROp():
    class SKLROpInner(SKSupervisedOp):
        model_class = chariots.versioning._versioned_field.VersionedField(LinearRegression, chariots.versioning._version_type.VersionType.MINOR)

    return SKLROpInner


def test_sk_training_pipeline(SKLROp, YOp, XTrainOp, tmpdir):
    train_pipe = Pipeline([
        chariots.nodes._node.Node(XTrainOp(), output_nodes="x_train"),
        chariots.nodes._node.Node(YOp(), output_nodes="y_train"),
        chariots.nodes._node.Node(SKLROp(mode=MLMode.FIT), input_nodes=["x_train", "y_train"])
    ], "train")
    pred_pipe = Pipeline([
        chariots.nodes._node.Node(SKLROp(mode=MLMode.PREDICT), input_nodes=["__pipeline_input__"],
                                  output_nodes="__pipeline_output__")
    ], "pred")
    my_app = app.Chariot(app_pipelines=[train_pipe, pred_pipe], path=str(tmpdir), import_name="my_app")

    test_client = TestClient(my_app)
    prior_versions_train = test_client.pipeline_versions(train_pipe)
    prior_versions_pred = test_client.pipeline_versions(pred_pipe)
    test_client.request(train_pipe)
    test_client.save_pipeline(train_pipe)
    test_client.load_pipeline(pred_pipe)
    posterior_versions_train = test_client.pipeline_versions(train_pipe)
    posterior_versions_pred = test_client.pipeline_versions(pred_pipe)
    response = test_client.request(pred_pipe, pipeline_input=[[100], [101], [102]])

    assert len(response.value) == 3
    for i, individual_value in enumerate(response.value):
        assert abs(101 + i - individual_value) < 1e-5

    # testing version update
    lrop_train = train_pipe.node_for_name["sklropinner"]
    lrop_pred = pred_pipe.node_for_name["sklropinner"]
    assert prior_versions_train[lrop_train]== prior_versions_pred[lrop_pred]
    assert posterior_versions_train[lrop_train] == posterior_versions_pred[lrop_pred]
    assert prior_versions_pred[lrop_pred] < posterior_versions_pred[lrop_pred]


@pytest.fixture
def PCAOp():
    class PCAInner(SKUnsupervisedOp):
        training_update_version = chariots.versioning._version_type.VersionType.MAJOR
        model_parameters = chariots.versioning._versioned_field_dict.VersionedFieldDict(
            chariots.versioning._version_type.VersionType.MAJOR, {
            "n_components": 2,
        })
        model_class = chariots.versioning._versioned_field.VersionedField(PCA, chariots.versioning._version_type.VersionType.MAJOR)

    return PCAInner


@pytest.fixture
def XTrainOpL():
    class XTrainOpInner(BaseOp):

        def execute(self):
            return np.array([range(10), range(1, 11), range(2, 12)]).T
    return XTrainOpInner


def test_complex_sk_training_pipeline(SKLROp, YOp, XTrainOpL, PCAOp, tmpdir):

    train_transform = Pipeline([
        chariots.nodes._node.Node(XTrainOpL(), output_nodes="x_raw"),
        chariots.nodes._node.Node(PCAOp(mode=MLMode.FIT), input_nodes=["x_raw"], output_nodes="x_train"),
    ], "train_pca")
    train_pipe = Pipeline([
        chariots.nodes._node.Node(XTrainOpL(), output_nodes="x_raw"),
        chariots.nodes._node.Node(YOp(), output_nodes="y_train"),
        chariots.nodes._node.Node(PCAOp(mode=MLMode.PREDICT), input_nodes=["x_raw"], output_nodes="x_train"),
        chariots.nodes._node.Node(SKLROp(mode=MLMode.FIT), input_nodes=["x_train", "y_train"])
    ], "train")
    pred_pipe = Pipeline([
        chariots.nodes._node.Node(PCAOp(mode=MLMode.PREDICT), input_nodes=["__pipeline_input__"], output_nodes="x_train"),
        chariots.nodes._node.Node(SKLROp(mode=MLMode.PREDICT), input_nodes=["x_train"], output_nodes="__pipeline_output__")
    ], "pred")
    my_app = app.Chariot(app_pipelines=[train_transform, train_pipe, pred_pipe],
                         path=str(tmpdir), import_name="my_app")

    test_client = TestClient(my_app)
    test_client.request(train_transform)
    test_client.save_pipeline(train_transform)
    test_client.load_pipeline(train_pipe)
    test_client.request(train_pipe)
    test_client.save_pipeline(train_pipe)
    test_client.load_pipeline(pred_pipe)
    response = test_client.request(pred_pipe, pipeline_input=[[100, 101, 102], [101, 102, 103], [102, 103, 104]])

    assert len(response.value) == 3
    for i, individual_value in enumerate(response.value):
        assert abs(101 + i - individual_value) < 1e-5

    test_client.request(train_transform)
    test_client.save_pipeline(train_transform)
    with pytest.raises(VersionError):
        test_client.load_pipeline(pred_pipe)


def test_fit_predict_pipeline_reload(SKLROp, YOp, XTrainOpL, PCAOp, tmpdir):

    train_pca = Pipeline([
        chariots.nodes._node.Node(XTrainOpL(), output_nodes="x_raw"),
        chariots.nodes._node.Node(PCAOp(mode=MLMode.FIT), input_nodes=["x_raw"]),
        ], "train_pca")
    train_rf = Pipeline([
        chariots.nodes._node.Node(XTrainOpL(), output_nodes="x_raw"),
        chariots.nodes._node.Node(YOp(), output_nodes="y_train"),
        chariots.nodes._node.Node(PCAOp(mode=MLMode.PREDICT), input_nodes=["x_raw"], output_nodes="x_train"),
        chariots.nodes._node.Node(SKLROp(mode=MLMode.FIT), input_nodes=["x_train", "y_train"])
    ], "train_rf")
    pred_pipe = Pipeline([
        chariots.nodes._node.Node(PCAOp(mode=MLMode.PREDICT), input_nodes=["__pipeline_input__"], output_nodes="x_train"),
        chariots.nodes._node.Node(SKLROp(mode=MLMode.PREDICT), input_nodes=["x_train"], output_nodes="__pipeline_output__")
    ], "pred")
    my_app = app.Chariot(app_pipelines=[train_pca, train_rf, pred_pipe],
                         path=str(tmpdir), import_name="my_app")

    test_client = TestClient(my_app)

    # test that the train save load is working
    test_client.request(train_pca)
    test_client.save_pipeline(train_pca)
    test_client.load_pipeline(train_rf)
    test_client.request(train_rf)
    test_client.save_pipeline(train_rf)
    test_client.load_pipeline(pred_pipe)
    response = test_client.request(pred_pipe, pipeline_input=[[100, 101, 102], [101, 102, 103], [102, 103, 104]])

    assert len(response.value) == 3
    for i, individual_value in enumerate(response.value):
        assert abs(101 + i - individual_value) < 1e-5

    # test that that retrain is possible
    test_client.request(train_pca)
    test_client.save_pipeline(train_pca)
    test_client.load_pipeline(train_rf)
    test_client.request(train_rf)
    test_client.save_pipeline(train_rf)
    test_client.load_pipeline(pred_pipe)
    response = test_client.request(pred_pipe, pipeline_input=[[100, 101, 102], [101, 102, 103], [102, 103, 104]])

    assert len(response.value) == 3
    for i, individual_value in enumerate(response.value):
        assert abs(101 + i - individual_value) < 1e-5

    # test that that wrong loading is raising
    test_client.request(train_pca)
    test_client.save_pipeline(train_pca)
    with pytest.raises(VersionError):
        test_client.load_pipeline(pred_pipe)
    response = test_client.request(pred_pipe, pipeline_input=[[100, 101, 102], [101, 102, 103], [102, 103, 104]])

    assert len(response.value) == 3
    for i, individual_value in enumerate(response.value):
        assert abs(101 + i - individual_value) < 1e-5

