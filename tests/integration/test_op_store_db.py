import os

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from chariots import Pipeline, Chariots, TestClient
from chariots.op_store import models
from chariots.op_store._op_store_client import TestOpStoreClient


@pytest.fixture
def root_path(tmpdir):
    return str(tmpdir)


@pytest.fixture
def op_store_client(root_path: str):
    client = TestOpStoreClient(path=root_path)
    client.server.db.create_all()
    return client


@pytest.fixture
def db_uri(op_store_client: TestOpStoreClient):
    return 'sqlite:///{}'.format(op_store_client.db_path)


@pytest.fixture
def ops_path(root_path: str):
    return os.path.join(root_path, 'ops')


@pytest.fixture
def session_func(db_uri: str):
    engine = create_engine(db_uri)
    return sessionmaker(bind=engine)


def do_pipeline_initialization_test(train, pred, session, ops_path):
    # testing all the ops are present and their versions are saved.
    for node_name, node in train.node_for_name.items():
        db_op = session.query(models.DBOp).filter(models.DBOp.op_name == node_name).one()
        db_version = session.query(models.DBVersion).filter(models.DBVersion.op_id == db_op.id).one()
        assert db_version.to_chariots_version() == node.node_version

    for node_name, node in pred.node_for_name.items():
        db_op = session.query(models.DBOp).filter(models.DBOp.op_name == node_name).one()
        db_version = session.query(models.DBVersion).filter(models.DBVersion.op_id == db_op.id).one()
        assert db_version.to_chariots_version() == node.node_version

    # testing links
    links = [
        ('fromarray', 'sklrop', pred.node_for_name['sklrop'].node_version),
        ('sklrop', 'yop', train.node_for_name['yop'].node_version),
        ('sklrop', 'xtrainopinner', train.node_for_name['xtrainopinner'].node_version)
    ]

    for downstream, upstream, upstream_version in links:
        downstream_db_op = session.query(models.DBOp).filter(models.DBOp.op_name == downstream).one()
        upstream_db_op = session.query(models.DBOp).filter(models.DBOp.op_name == upstream).one()
        upstream_db_version = session.query(models.DBVersion).filter(models.DBVersion.op_id == upstream_db_op.id).one()
        assert upstream_db_version.to_chariots_version() == upstream_version
        db_link = session.query(
            models.DBValidatedLink
        ).filter(
            models.DBValidatedLink.downstream_op_id == downstream_db_op.id
        ).filter(
            models.DBValidatedLink.upstream_op_id == upstream_db_op.id
        ).one()
        assert db_link.upstream_op_version_id == upstream_db_version.id

    # testing ops
    model_path = os.path.join(ops_path, 'models', 'sklrop')
    assert os.path.exists(model_path)
    assert os.path.isdir(model_path)
    assert len(list(os.listdir(model_path))) == 1


def test_pipeline_initialization(basic_sk_pipelines: Pipeline, op_store_client: TestOpStoreClient,
                                 session_func: sessionmaker, ops_path: str):
    train, pred = basic_sk_pipelines
    Chariots([train, pred], op_store_client=op_store_client, import_name='app')

    session = session_func()
    do_pipeline_initialization_test(train, pred, session, ops_path)


def test_pipeline_initialization_already_initialized(basic_sk_pipelines: Pipeline, op_store_client: TestOpStoreClient,
                                                     session_func: sessionmaker, ops_path):
    train, pred = basic_sk_pipelines
    app = Chariots([train, pred], op_store_client=op_store_client, import_name='app')
    del app
    app = Chariots([train, pred], op_store_client=op_store_client, import_name='app')

    session = session_func()
    do_pipeline_initialization_test(train, pred, session, ops_path)


def test_pipeline_saving(basic_sk_pipelines: Pipeline, op_store_client: TestOpStoreClient,
                         session_func: sessionmaker, ops_path: str):
    train, pred = basic_sk_pipelines

    old_version = train.node_for_name['sklrop'].node_version
    app = Chariots([train, pred], op_store_client=op_store_client, import_name='app')

    test_client = TestClient(app)

    response = test_client.call_pipeline(train)
    test_client.save_pipeline(train)
    print(response.versions)
    new_version = response.versions[train.node_for_name['sklrop']]

    session = session_func()

    for node_name, node in train.node_for_name.items():
        db_op = session.query(models.DBOp).filter(models.DBOp.op_name == node_name).one()
        if node_name != 'sklrop':
            db_version = session.query(models.DBVersion).filter(models.DBVersion.op_id == db_op.id).one()
            assert db_version.to_chariots_version() == node.node_version
            continue
        db_versions = list(session.query(models.DBVersion).filter(models.DBVersion.op_id == db_op.id))
        assert len(db_versions) == 2
        assert {db_version.to_chariots_version().major
                for db_version in db_versions} == {old_version.major, new_version.major}
        assert {db_version.to_chariots_version().minor
                for db_version in db_versions} == {old_version.minor, new_version.minor}
        assert {db_version.to_chariots_version().patch
                for db_version in db_versions} == {old_version.patch, new_version.patch}

    # testing saved ops
    model_path = os.path.join(ops_path, 'models', 'sklrop')
    assert os.path.exists(model_path)
    assert os.path.isdir(model_path)
    assert len(list(os.listdir(model_path))) == 2
