import os
from collections import Counter
from typing import Type

import pytest
from sqlalchemy import create_engine, alias
from sqlalchemy.orm import sessionmaker

from chariots import versioning
from chariots.pipelines import Pipeline, nodes, ops
from chariots.op_store import models
from chariots.testing import TestOpStoreClient


class FakeOp:

    def __init__(self, name):
        self.name = name

class DumbOp(ops.BaseOp):

    def execute(self):
        return 'Samwise Gamgee'


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


def test_register_valid_link_no_ops(op_store_client: TestOpStoreClient, session_func: sessionmaker):
    downstream_op_name = 'downstream'
    upstream_op_name = 'upstream'
    upstream_version = versioning.Version()

    op_store_client.register_valid_link(downstream_op_name, upstream_op_name, upstream_op_version=upstream_version)

    session = session_func()
    ops = list(session.query(models.DBOp))

    assert len(ops) == 2
    assert set(op.op_name for op in ops) == {downstream_op_name, upstream_op_name}
    upstream_op = [op for op in ops if op.op_name == upstream_op_name][0]
    downstream_op = [op for op in ops if op.op_name == downstream_op_name][0]

    versions = list(session.query(models.DBVersion))
    assert len(versions) == 1
    version = versions[0]
    assert version.op_id == upstream_op.id
    assert version.major_hash == upstream_version.major
    assert version.minor_hash == upstream_version.minor
    assert version.patch_hash == upstream_version.patch
    assert version.major_version_number == 1
    assert version.minor_version_number == 0
    assert version.minor_version_number == 0

    links = list(session.query(models.DBValidatedLink))
    assert len(links) == 1
    link = links[0]
    assert link.upstream_op_id == upstream_op.id
    assert link.downstream_op_id == downstream_op.id
    assert link.upstream_op_version_id == version.id


def test_register_validated_lin_ops_present(op_store_client: TestOpStoreClient, session_func: sessionmaker):

    # creating first link
    downstream_op_name = 'downstream'
    upstream_op_name = 'upstream'
    upstream_version = versioning.Version()
    op_store_client.register_valid_link(downstream_op_name, upstream_op_name, upstream_op_version=upstream_version)

    # getting old versions and link info
    session = session_func()
    old_db_version = session.query(models.DBVersion).one()
    old_db_link = session.query(models.DBValidatedLink).one()

    # creating new version and updating link
    new_version = upstream_version + versioning.Version()
    assert new_version > upstream_version
    op_store_client.register_valid_link(downstream_op_name, upstream_op_name, upstream_op_version=new_version)

    # testing new link
    ops = list(session.query(models.DBOp))
    assert len(ops) == 2
    assert set(op.op_name for op in ops) == {downstream_op_name, upstream_op_name}
    upstream_op = [op for op in ops if op.op_name == upstream_op_name][0]
    downstream_op = [op for op in ops if op.op_name == downstream_op_name][0]

    versions = list(session.query(models.DBVersion))
    assert len(versions) == 2
    assert set(version.op_id for version in versions) == {upstream_op.id}
    new_db_version = session.query(models.DBVersion).filter(models.DBVersion.id != old_db_version.id).one()

    assert new_db_version.major_hash == new_version.major
    assert new_db_version.minor_hash == new_version.minor
    assert new_db_version.patch_hash == new_version.patch
    assert new_db_version.major_version_number == 2
    assert new_db_version.minor_version_number == 0
    assert new_db_version.minor_version_number == 0

    links = list(session.query(models.DBValidatedLink))
    assert len(links) == 2
    for link in links:
        assert link.upstream_op_id == upstream_op.id
        assert link.downstream_op_id == downstream_op.id
    new_db_link = session.query(models.DBValidatedLink).filter(models.DBValidatedLink.id != old_db_link.id).one()
    assert new_db_link.upstream_op_version_id == new_db_version.id


def test_get_validated_links(op_store_client: TestOpStoreClient):

    # creating some links
    first_version = versioning.Version()
    op_store_client.register_valid_link('downstream', 'upstream', upstream_op_version=first_version)

    other_version = first_version + versioning.Version()
    assert other_version != first_version
    op_store_client.register_valid_link('foo', 'upstream', upstream_op_version=other_version)

    new_upstream_version = other_version + versioning.Version()
    assert new_upstream_version != first_version
    assert new_upstream_version != other_version
    op_store_client.register_valid_link('downstream', 'upstream', upstream_op_version=new_upstream_version)

    # getting the links
    validated_links = op_store_client.get_validated_links('downstream', 'upstream')
    assert validated_links == {first_version, new_upstream_version}


def test_save_op_bytes(op_store_client: TestOpStoreClient, ops_path):

    op_bytes = 'one ring to bind them all and in the darkness bring them'.encode('utf-8')
    version = versioning.Version()

    fake_op = FakeOp('the_one_op')

    # saving the op and the version
    db_op = op_store_client.server.get_or_register_db_op(fake_op.name)
    op_store_client.server.get_or_register_db_version(version, db_op.id)

    # saving op
    op_store_client.save_op_bytes(fake_op, version, op_bytes)

    # checking the op was saved
    op_path = os.path.join(ops_path, 'models', fake_op.name, str(version))
    assert os.path.exists(op_path)
    with open(op_path, 'rb') as op_bytes_file:
        assert op_bytes_file.read() == op_bytes


def test_get_saved_op_bytes(op_store_client: TestOpStoreClient):

    op_bytes = 'one ring to bind them all and in the darkness bring them'.encode('utf-8')
    version = versioning.Version()

    fake_op = FakeOp('the_one_op')

    # saving the op and the version
    db_op = op_store_client.server.get_or_register_db_op(fake_op.name)
    op_store_client.server.get_or_register_db_version(version, db_op.id)

    # saving op
    op_store_client.save_op_bytes(fake_op, version, op_bytes)
    assert op_store_client.get_op_bytes_for_version(fake_op, version) == op_bytes


@pytest.fixture
def small_test_pipeline():

    return Pipeline([
        nodes.Node(DumbOp(), output_nodes=['__pipeline_output__'])
    ], name='cooking_pipeline')


def get_all_links(session, pipeline_id):
    links = session.query(
       models.DBPipelineLink, models.DBOp
    ).filter(
        models.DBPipelineLink.pipeline_id == pipeline_id
    ).filter(
        models.DBPipelineLink.upstream_op_id == models.DBOp.id
    ).all()
    res = []
    for link, upstream in links:
        if link.downstream_op_id is None:
            res.append((link, upstream.op_name, None))
            continue
        downstream = session.query(models.DBOp).filter(models.DBOp.id == link.downstream_op_id).one()
        res.append((link, upstream.op_name, downstream.op_name))
    return res


def test_register_new_pipeline(op_store_client: TestOpStoreClient, small_test_pipeline: Pipeline,
                               session_func: sessionmaker):

    # registering the op and the pipeline
    version = versioning.Version()
    op_store_client.register_valid_link(None, upstream_op_name=DumbOp().name, upstream_op_version=version)
    op_store_client.register_new_pipeline(small_test_pipeline)

    # test the registration
    session = session_func()
    db_op = session.query(models.DBOp).one()
    db_pipeline = session.query(models.DBPipeline).one()
    assert db_pipeline.pipeline_name == 'cooking_pipeline'

    all_ops = get_all_links(session, db_pipeline.id)
    assert len(all_ops) == 1
    op_link, upstream_op_name, downstream_op_name = all_ops[0]
    assert upstream_op_name == db_op.op_name
    assert downstream_op_name == None


def test_register_new_pipeline_complex_dag(op_store_client: TestOpStoreClient, session_func: sessionmaker):

    # defining the necesasry op and pipeline
    class OpwithInput(ops.BaseOp):

        def execute(self, input1='sad', input2=None):
            if input2 is None:
                return input1 + '_foo'
            return input1 + input2 + '_bar'

    test_pipeline = Pipeline([
        nodes.Node(OpwithInput(), input_nodes=['__pipeline_input__'], output_nodes=['input_1']),
        nodes.Node(OpwithInput(), output_nodes=['input_2']),
        nodes.Node(OpwithInput(), input_nodes=['input_1', 'input_2'], output_nodes=['__pipeline_output__']),
    ], name='complex_dag_pipeline')

    # registering the ops and pipeline
    for upstream_node, downstream_node in test_pipeline.get_all_op_links():
        if downstream_node:
            op_store_client.register_valid_link(downstream_node.name, upstream_node.name, upstream_node.node_version)
    op_store_client.register_new_pipeline(test_pipeline)

    # test the registration
    session = session_func()
    db_op = session.query(models.DBOp).one()
    assert db_op.op_name == 'opwithinput'
    db_pipeline = session.query(models.DBPipeline).one()
    assert db_pipeline.pipeline_name == 'complex_dag_pipeline'

    all_links = get_all_links(session, db_pipeline.id)
    assert len(all_links) == 3
    assert Counter([(link[1], link[2]) for link in all_links]) == {
        ('opwithinput', 'opwithinput'): 2,
        ('opwithinput', None): 1
    }



def test_pipeline_exists(op_store_client: TestOpStoreClient, small_test_pipeline: Pipeline):

    assert not op_store_client.pipeline_exists(small_test_pipeline.name)
    version = versioning.Version()
    op_store_client.register_valid_link(None, upstream_op_name=DumbOp().name, upstream_op_version=version)
    op_store_client.register_new_pipeline(small_test_pipeline)
    assert op_store_client.pipeline_exists(small_test_pipeline.name)
