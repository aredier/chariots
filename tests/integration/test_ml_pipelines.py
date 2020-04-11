"""module that tests ml pipelines and the different setups possible"""
import pytest
import numpy as np
from sklearn.linear_model import LinearRegression

from chariots.ml import MLMode, BaseMLOp
from chariots.pipelines import Pipeline, PipelinesServer
from chariots.pipelines.nodes import Node
from chariots.testing import TestPipelinesClient
from chariots.errors import VersionError
from chariots._helpers.test_helpers import XTrainOpL, PCAOp, YOp, SKLROp, FromArray


@pytest.fixture
def LROp():  # pylint: disable=invalid-name
    """fixture of a linear regression model by reimplementing it (subclassing `MLOp` rather than `SKSupervisedOp`)"""

    class LROpInner(BaseMLOp):  # pylint: disable=arguments-differ
        """inner op of the fixture"""

        def fit(self, x_train, y_train):  # pylint: disable=arguments-differ
            self._model.fit(np.array(x_train).reshape(-1, 1), y_train)

        def predict(self, x_pred):  # pylint: disable=arguments-differ
            if not isinstance(x_pred, list):
                x_pred = [x_pred]
            return int(self._model.predict(np.array(x_pred).reshape(-1, 1))[0])

        def _init_model(self):
            return LinearRegression()

    return LROpInner


def test_raw_training_pipeline(  # pylint: disable=invalid-name, invalid-name
        XTrainOp, tmpdir,
        LROp,  # pylint: disable=redefined-outer-name
        opstore_func):
    """test basic training and prediction pipeline"""
    train_pipe = Pipeline([
        Node(XTrainOp(), output_nodes='x_train'),
        Node(YOp(), output_nodes='y_train'),
        Node(LROp(mode=MLMode.FIT), input_nodes=['x_train', 'y_train'])
    ], 'train')
    pred_pipe = Pipeline([
        Node(LROp(mode=MLMode.PREDICT), input_nodes=['__pipeline_input__'],
             output_nodes='__pipeline_output__')
    ], 'pred')
    my_app = PipelinesServer(app_pipelines=[train_pipe, pred_pipe],
                             op_store_client=opstore_func(tmpdir), import_name='my_app')

    test_client = TestPipelinesClient(my_app)
    prior_versions_train = test_client.pipeline_versions(train_pipe)
    prior_versions_pred = test_client.pipeline_versions(pred_pipe)
    test_client.call_pipeline(train_pipe)
    test_client.save_pipeline(train_pipe)
    test_client.load_pipeline(pred_pipe)
    posterior_versions_train = test_client.pipeline_versions(train_pipe)
    posterior_versions_pred = test_client.pipeline_versions(pred_pipe)
    response = test_client.call_pipeline(pred_pipe, pipeline_input=3)

    assert response.value == 4

    # testing verison uodate
    lrop_train = train_pipe.node_for_name['lropinner']
    lrop_pred = pred_pipe.node_for_name['lropinner']
    assert prior_versions_train[lrop_train.name] == prior_versions_pred[lrop_pred.name]
    assert posterior_versions_train[lrop_train.name] == posterior_versions_pred[lrop_pred.name]
    assert prior_versions_pred[lrop_pred.name] < posterior_versions_pred[lrop_pred.name]


def test_sk_training_pipeline(opstore_func, tmpdir, basic_sk_pipelines):
    """test a train/predict pipelines using sci-kit learn ops"""
    train_pipe, pred_pipe = basic_sk_pipelines
    my_app = PipelinesServer(app_pipelines=[train_pipe, pred_pipe], op_store_client=opstore_func(tmpdir),
                             import_name='my_app')

    test_client = TestPipelinesClient(my_app)
    prior_versions_train = test_client.pipeline_versions(train_pipe)
    prior_versions_pred = test_client.pipeline_versions(pred_pipe)
    test_client.call_pipeline(train_pipe)
    test_client.save_pipeline(train_pipe)
    test_client.load_pipeline(pred_pipe)
    posterior_versions_train = test_client.pipeline_versions(train_pipe)
    posterior_versions_pred = test_client.pipeline_versions(pred_pipe)
    response = test_client.call_pipeline(pred_pipe, pipeline_input=[[100], [101], [102]])

    assert len(response.value) == 3
    for i, individual_value in enumerate(response.value):
        assert abs(101 + i - individual_value) < 1e-5

    # testing version update
    lrop_train = train_pipe.node_for_name['sklrop']
    lrop_pred = pred_pipe.node_for_name['sklrop']
    assert prior_versions_train[lrop_train.name] == prior_versions_pred[lrop_pred.name]
    assert posterior_versions_train[lrop_train.name] == posterior_versions_pred[lrop_pred.name]
    assert prior_versions_pred[lrop_pred.name] < posterior_versions_pred[lrop_pred.name]


def test_complex_sk_training_pipeline(complex_sk_pipelines, opstore_func, tmpdir):
    """tests complex ml pipelines (with a pca pipelines, a model training pipeline and a prediction pipeline)"""

    train_transform, train_pipe, pred_pipe = complex_sk_pipelines
    my_app = PipelinesServer(app_pipelines=[train_transform, train_pipe, pred_pipe],
                             op_store_client=opstore_func(tmpdir), import_name='my_app')

    test_client = TestPipelinesClient(my_app)
    test_client.call_pipeline(train_transform)
    test_client.save_pipeline(train_transform)
    test_client.load_pipeline(train_pipe)
    test_client.call_pipeline(train_pipe)
    test_client.save_pipeline(train_pipe)
    test_client.load_pipeline(pred_pipe)
    response = test_client.call_pipeline(pred_pipe, pipeline_input=[[100, 101, 102], [101, 102, 103], [102, 103, 104]])

    assert len(response.value) == 3
    for i, individual_value in enumerate(response.value):
        assert abs(101 + i - individual_value) < 1e-5

    test_client.call_pipeline(train_transform)
    test_client.save_pipeline(train_transform)
    with pytest.raises(VersionError):
        test_client.load_pipeline(pred_pipe)


def test_fit_predict_pipeline_reload(opstore_func, tmpdir):
    """
    tests a complex pipeline system with a pca-train, model-train and prediction pipeline. The aim of this test is
    to check that the version check and reload behavior works
    """

    train_pca = Pipeline([
        Node(XTrainOpL(), output_nodes='x_raw'),
        Node(PCAOp(mode=MLMode.FIT), input_nodes=['x_raw']),
    ], 'train_pca')
    train_rf = Pipeline([
        Node(XTrainOpL(), output_nodes='x_raw'),
        Node(YOp(), output_nodes='y_train'),
        Node(PCAOp(mode=MLMode.PREDICT), input_nodes=['x_raw'], output_nodes='x_train'),
        Node(SKLROp(mode=MLMode.FIT), input_nodes=['x_train', 'y_train'])
    ], 'train_rf')
    pred_pipe = Pipeline([
        Node(PCAOp(mode=MLMode.PREDICT), input_nodes=['__pipeline_input__'], output_nodes='x_train'),
        Node(SKLROp(mode=MLMode.PREDICT), input_nodes=['x_train'], output_nodes='pred'),
        Node(FromArray(), input_nodes=['pred'], output_nodes='__pipeline_output__')
    ], 'pred')

    my_app = PipelinesServer(app_pipelines=[train_pca, train_rf, pred_pipe],
                             op_store_client=opstore_func(tmpdir), import_name='my_app')
    test_client = TestPipelinesClient(my_app)

    # test that the train save load is working
    test_client.call_pipeline(train_pca)
    test_client.save_pipeline(train_pca)
    test_client.load_pipeline(train_rf)
    test_client.call_pipeline(train_rf)
    test_client.save_pipeline(train_rf)
    test_client.load_pipeline(pred_pipe)
    response = test_client.call_pipeline(pred_pipe, pipeline_input=[[100, 101, 102], [101, 102, 103], [102, 103, 104]])
    assert len(response.value) == 3
    for i, individual_value in enumerate(response.value):
        assert abs(101 + i - individual_value) < 1e-5

    # test that that retrain is possible
    test_client.call_pipeline(train_pca)
    test_client.save_pipeline(train_pca)
    test_client.load_pipeline(train_rf)
    test_client.call_pipeline(train_rf)
    test_client.save_pipeline(train_rf)
    test_client.load_pipeline(pred_pipe)
    response = test_client.call_pipeline(pred_pipe, pipeline_input=[[100, 101, 102], [101, 102, 103], [102, 103, 104]])

    assert len(response.value) == 3
    for i, individual_value in enumerate(response.value):
        assert abs(101 + i - individual_value) < 1e-5

    # test that that wrong loading is raising
    test_client.call_pipeline(train_pca)
    test_client.save_pipeline(train_pca)
    with pytest.raises(VersionError):
        test_client.load_pipeline(pred_pipe)
    response = test_client.call_pipeline(pred_pipe, pipeline_input=[[100, 101, 102], [101, 102, 103], [102, 103, 104]])

    assert len(response.value) == 3
    for i, individual_value in enumerate(response.value):
        assert abs(101 + i - individual_value) < 1e-5


def test_multiple_models_per_pipeline(opstore_func, tmpdir):
    """
    tests a complex pipeline system with a pca-train, model-train and prediction pipeline. The aim of this test is
    to check that the version check and reload behavior works
    """

    def make_prediction_test(client, train_pipe, pred_pipe):
        client.call_pipeline(train_pipe)
        client.save_pipeline(train_pipe)
        client.load_pipeline(pred_pipe)
        response = test_client.call_pipeline(pred_pipe,
                                             pipeline_input=[[100, 101, 102], [101, 102, 103], [102, 103, 104]])

        assert len(response.value) == 3
        for i, individual_value in enumerate(response.value):
            assert abs(101 + i - individual_value) < 1e-5

    train = Pipeline([
        Node(XTrainOpL(), output_nodes='x_raw'),
        Node(YOp(), output_nodes='y_train'),
        Node(PCAOp(mode=MLMode.FIT_PREDICT), input_nodes=['x_raw'], output_nodes='x_train'),
        Node(SKLROp(mode=MLMode.FIT), input_nodes=['x_train', 'y_train'])
    ], 'train')

    pred_pipe = Pipeline([
        Node(PCAOp(mode=MLMode.PREDICT), input_nodes=['__pipeline_input__'], output_nodes='x_train'),
        Node(SKLROp(mode=MLMode.PREDICT), input_nodes=['x_train'], output_nodes='pred'),
        Node(FromArray(), input_nodes=['pred'], output_nodes='__pipeline_output__')
    ], 'pred')
    my_app = PipelinesServer(app_pipelines=[train, pred_pipe],
                             op_store_client=opstore_func(tmpdir), import_name='my_app')

    test_client = TestPipelinesClient(my_app)

    # test that the train save load is working
    make_prediction_test(test_client, train, pred_pipe)

    # test that that retrain is possible
    make_prediction_test(test_client, train, pred_pipe)
