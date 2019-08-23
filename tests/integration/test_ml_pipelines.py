import pytest
import numpy as np
from sklearn.decomposition import PCA
from sklearn.linear_model import LinearRegression

from chariots.backend import app
from chariots.backend.client import TestClient
from chariots.core import pipelines, nodes, versioning
from chariots.core.ops import AbstractOp
from chariots.helpers.errors import VersionError
from chariots.ml import ml_op, sklearn_op

@pytest.fixture
def LROp():
    class LROpInner(ml_op.MLOp):

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
    train_pipe = pipelines.Pipeline([
        nodes.Node(XTrainOp(), output_nodes="x_train"),
        nodes.Node(YOp(), output_nodes="y_train"),
        nodes.Node(LROp(mode=ml_op.MLMode.FIT), input_nodes=["x_train", "y_train"])
    ], "train")
    pred_pipe = pipelines.Pipeline([
        nodes.Node(LROp(mode=ml_op.MLMode.PREDICT), input_nodes=["__pipeline_input__"],
                   output_nodes="__pipeline_output__")
    ], "pred")
    my_app = app.Chariot(app_pipelines=[train_pipe, pred_pipe], path=tmpdir, import_name="my_app")

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
    class SKLROpInner(sklearn_op.SKSupervisedModel):
        model_class = versioning.VersionedField(LinearRegression, versioning.VersionType.MINOR)

    return SKLROpInner


def test_sk_training_pipeline(SKLROp, YOp, XTrainOp, tmpdir):
    train_pipe = pipelines.Pipeline([
        nodes.Node(XTrainOp(), output_nodes="x_train"),
        nodes.Node(YOp(), output_nodes="y_train"),
        nodes.Node(SKLROp(mode=ml_op.MLMode.FIT), input_nodes=["x_train", "y_train"])
    ], "train")
    pred_pipe = pipelines.Pipeline([
        nodes.Node(SKLROp(mode=ml_op.MLMode.PREDICT), input_nodes=["__pipeline_input__"],
                   output_nodes="__pipeline_output__")
    ], "pred")
    my_app = app.Chariot(app_pipelines=[train_pipe, pred_pipe], path=tmpdir, import_name="my_app")

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
    class PCAInner(sklearn_op.SKUnsupervisedModel):
        training_update_version = versioning.VersionType.MAJOR
        model_parameters = versioning.VersionedFieldDict(versioning.VersionType.MAJOR, {
            "n_components": 2,
        })
        model_class = versioning.VersionedField(PCA, versioning.VersionType.MAJOR)

    return PCAInner


@pytest.fixture
def XTrainOpL():
    class XTrainOpInner(AbstractOp):

        def __call__(self):
            return np.array([range(10), range(1, 11), range(2, 12)]).T
    return XTrainOpInner


def test_complex_sk_training_pipeline(SKLROp, YOp, XTrainOpL, PCAOp, tmpdir):

    train_transform = pipelines.Pipeline([
        nodes.Node(XTrainOpL(), output_nodes="x_raw"),
        nodes.Node(PCAOp(mode=ml_op.MLMode.FIT), input_nodes=["x_raw"], output_nodes="x_train"),
    ], "train_pca")
    train_pipe = pipelines.Pipeline([
        nodes.Node(XTrainOpL(), output_nodes="x_raw"),
        nodes.Node(YOp(), output_nodes="y_train"),
        nodes.Node(PCAOp(mode=ml_op.MLMode.PREDICT), input_nodes=["x_raw"], output_nodes="x_train"),
        nodes.Node(SKLROp(mode=ml_op.MLMode.FIT), input_nodes=["x_train", "y_train"])
    ], "train")
    pred_pipe = pipelines.Pipeline([
        nodes.Node(PCAOp(mode=ml_op.MLMode.PREDICT), input_nodes=["__pipeline_input__"], output_nodes="x_train"),
        nodes.Node(SKLROp(mode=ml_op.MLMode.PREDICT), input_nodes=["x_train"], output_nodes="__pipeline_output__")
    ], "pred")
    my_app = app.Chariot(app_pipelines=[train_transform, train_pipe, pred_pipe],
                         path=tmpdir, import_name="my_app")

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

    train_pca = pipelines.Pipeline([
        nodes.Node(XTrainOpL(), output_nodes="x_raw"),
        nodes.Node(PCAOp(mode=ml_op.MLMode.FIT), input_nodes=["x_raw"]),
        ], "train_pca")
    train_rf = pipelines.Pipeline([
        nodes.Node(XTrainOpL(), output_nodes="x_raw"),
        nodes.Node(YOp(), output_nodes="y_train"),
        nodes.Node(PCAOp(mode=ml_op.MLMode.PREDICT), input_nodes=["x_raw"], output_nodes="x_train"),
        nodes.Node(SKLROp(mode=ml_op.MLMode.FIT), input_nodes=["x_train", "y_train"])
    ], "train_rf")
    pred_pipe = pipelines.Pipeline([
        nodes.Node(PCAOp(mode=ml_op.MLMode.PREDICT), input_nodes=["__pipeline_input__"], output_nodes="x_train"),
        nodes.Node(SKLROp(mode=ml_op.MLMode.PREDICT), input_nodes=["x_train"], output_nodes="__pipeline_output__")
    ], "pred")
    my_app = app.Chariot(app_pipelines=[train_pca, train_rf, pred_pipe],
                         path=tmpdir, import_name="my_app")

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

