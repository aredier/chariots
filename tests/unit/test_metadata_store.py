import os
import datetime as dt
import random

import pytest
from sqlalchemy.orm import sessionmaker


@pytest.fixture
def db_uri(tmpdir):
    return 'sqlite:///{}'.format(os.path.join(str(tmpdir), 'test.db'))


@pytest.fixture()
def models():
    from chariots import op_store
    return op_store.models


@pytest.fixture
def metadata_server(db_uri):
    from chariots.metadata import MetadataServer
    server = MetadataServer(db_uri)
    server.db.create_all()
    return server


@pytest.fixture
def metadata_schema(metadata_server):
    return metadata_server.schema


@pytest.fixture
def session_cls(models):
    return sessionmaker(bind=models.DBOp.query.session.bind)


@pytest.fixture
def op(session_cls, models):
    session = session_cls(expire_on_commit=False)
    op = models.DBOp(
        op_name='test'
    )
    session.add(op)
    session.commit()
    session.expunge_all()
    return op


@pytest.fixture
def pipeline(session_cls, models):
    session = session_cls(expire_on_commit=False)
    pipeline = models.DBPipeline(
        pipeline_name='test-pipeline'
    )
    session.add(pipeline)
    session.commit()
    return pipeline


@pytest.fixture
def pipeline_link(session_cls, pipeline, op, models):
    session = session_cls(expire_on_commit=False)
    link = models.DBPipelineLink(
        pipeline_id=pipeline.id,
        upstream_op_id=op.id,
        downstream_op_id=None,
    )
    session.add(link)
    session.commit()
    return session


@pytest.fixture
def version(session_cls, op, models):
    session = session_cls(expire_on_commit=False)
    version = models.DBVersion(
        op_id=op.id,
        version_time=dt.datetime.utcnow(),
        major_hash='0',
        major_version_number=0,
        minor_hash='1',
        minor_version_number=1,
        patch_hash='0',
        patch_version_number='0'
    )
    session.add(version)
    session.commit()
    session.expunge_all()
    return version


@pytest.fixture
def complex_ops(session_cls, models):
    op_1 = models.DBOp(op_name='op-1')
    op_2 = models.DBOp(op_name='op-2')
    op_3 = models.DBOp(op_name='op-3')
    op_4 = models.DBOp(op_name='op-4')

    session = session_cls(expire_on_commit=False)
    session.add(op_1)
    session.add(op_2)
    session.add(op_3)
    session.add(op_4)
    session.commit()
    return {
        1: op_1,
        2: op_2,
        3: op_3,
        4: op_4,
    }


@pytest.fixture
def complex_versions(session_cls, complex_ops, models):
    session = session_cls(expire_on_commit=False)
    versions = {}
    for op_number, op in complex_ops.items():
        op_version = models.DBVersion(
            op_id=op.id,
            major_hash=str(random.randint(0, 10)),
            minor_hash=str(random.randint(0, 10)),
            patch_hash=str(random.randint(0, 10)),
            major_version_number=random.randint(0, 10),
            minor_version_number=random.randint(0, 10),
            patch_version_number=random.randint(0, 10),
        )
        session.add(op_version)
        versions[op_number] = op_version
    session.commit()
    return versions


@pytest.fixture
def complex_links(session_cls, complex_ops, pipeline, models):
    session = session_cls()
    link_1 = models.DBPipelineLink(pipeline_id=pipeline.id, upstream_op_id=complex_ops[1].id,
                                   downstream_op_id=complex_ops[2].id)
    session.add(link_1)
    link_2 = models.DBPipelineLink(pipeline_id=pipeline.id, upstream_op_id=complex_ops[1].id,
                                   downstream_op_id=complex_ops[3].id)
    session.add(link_2)
    link_3 = models.DBPipelineLink(pipeline_id=pipeline.id, upstream_op_id=complex_ops[2].id,
                                   downstream_op_id=complex_ops[4].id)
    session.add(link_3)
    link_4 = models.DBPipelineLink(pipeline_id=pipeline.id, upstream_op_id=complex_ops[3].id,
                                   downstream_op_id=complex_ops[4].id)
    session.add(link_4)
    session.commit()
    return {
        1: link_1,
        2: link_2,
        3: link_3,
        4: link_4,
    }


@pytest.fixture
def complex_validated_links(session_cls, complex_versions, complex_ops, pipeline, models):
    session = session_cls()
    link_1 = models.DBValidatedLink(upstream_op_id=complex_ops[1].id,
                                    downstream_op_id=complex_ops[2].id, upstream_op_version_id=complex_versions[1].id)
    session.add(link_1)
    link_2 = models.DBValidatedLink(upstream_op_id=complex_ops[1].id,
                                    downstream_op_id=complex_ops[3].id, upstream_op_version_id=complex_versions[1].id)
    session.add(link_2)
    link_3 = models.DBValidatedLink(upstream_op_id=complex_ops[2].id, downstream_op_id=complex_ops[4].id,
                                    upstream_op_version_id=complex_versions[2].id)
    session.add(link_3)
    link_4 = models.DBValidatedLink(upstream_op_id=complex_ops[3].id, downstream_op_id=complex_ops[4].id,
                                    upstream_op_version_id=complex_versions[3].id)
    session.add(link_4)
    session.commit()
    return {
        1: link_1,
        2: link_2,
        3: link_3,
        4: link_4,
    }


def clean_db(session_cls, models):
    session = session_cls()
    session.query(models.DBValidatedLink).delete()
    session.query(models.DBVersion).delete()
    session.query(models.DBPipelineLink).delete()
    session.query(models.DBPipeline).delete()
    session.query(models.DBOp).delete()
    session.commit()


def test_op_query(metadata_schema, op, db_uri, session_cls, models):
    # clean_db(session_cls, models)
    metadata_ops = metadata_schema.execute("""
query{
   ops {
    edges {
      node {
        id
        opName
      }
    }
  }
}
""")
    assert len(metadata_ops.data['ops']['edges']) == 1
    assert metadata_ops.data['ops']['edges'][0]['node']['opName'] == op.op_name
    graphql_op_id = metadata_ops.data['ops']['edges'][0]['node']['id']
    # raise

    direct_query = metadata_schema.execute("""
query getOp ($id: ID!){
  op (id: $id){
    id
    opName
  }
}""", variables={'id': graphql_op_id})
    assert direct_query.data['op']['opName'] == op.op_name
    clean_db(session_cls, models)


def test_version_query(metadata_schema, op, version, db_uri, models, session_cls):
    all_versions = metadata_schema.execute("""
query{
  versions {
    edges{
      node {
        majorHash
        minorHash
        patchHash
        op{
          id
          opName
        }
      }
    }
  }
}
    """)

    assert len(all_versions.data['versions']['edges']) == 1
    assert all_versions.data['versions']['edges'][0]['node']['majorHash'] == version.major_hash
    assert all_versions.data['versions']['edges'][0]['node']['minorHash'] == version.minor_hash
    assert all_versions.data['versions']['edges'][0]['node']['patchHash'] == version.patch_hash
    assert all_versions.data['versions']['edges'][0]['node']['op']['opName'] == op.op_name
    op_id = all_versions.data['versions']['edges'][0]['node']['op']['id']

    op_query = metadata_schema.execute("""
query getOp ($id: ID!){
  op (id: $id){
    id
    opName
    versions{
      edges{
        node {
         majorHash
         minorHash
         patchHash
        }
      }
    }
  }
}
    """, variables={'id': op_id})
    assert op_query.data['op']['opName'] == op.op_name
    assert len(op_query.data['op']['versions']['edges']) == 1
    assert op_query.data['op']['versions']['edges'][0]['node']['majorHash'] == version.major_hash
    assert op_query.data['op']['versions']['edges'][0]['node']['minorHash'] == version.minor_hash
    assert op_query.data['op']['versions']['edges'][0]['node']['patchHash'] == version.patch_hash
    clean_db(session_cls, models)


pipelines_query_str = """
query {
  pipelines{
    edges{
      node{
        id
        pipelineName
        links{
          edges{
            node{
              upstreamOp{
                opName
              }
              downstreamOp{
                opName
              }
            }
          }
        }
      }
    }
  }
}
"""

single_pipeline_query_str = """
query getPipeline($id: ID!){
  pipeline(id: $id){
    pipelineName
    links{
      edges{
        node{
          upstreamOp{
            opName
          }
          downstreamOp{
            opName
          }
        }
      }
    }
  }
}
"""


def test_pipeline_query(metadata_schema, op, pipeline, pipeline_link, models, session_cls):
    # querying all the pipelines
    pipeline_query = metadata_schema.execute(pipelines_query_str)
    assert not pipeline_query.errors
    assert len(pipeline_query.data['pipelines']['edges']) == 1
    assert pipeline_query.data['pipelines']['edges'][0]['node']['pipelineName'] == pipeline.pipeline_name
    assert len(pipeline_query.data['pipelines']['edges'][0]['node']['links']['edges']) == 1
    assert pipeline_query.data['pipelines']['edges'][0]['node']['links']['edges'][0]['node']['upstreamOp'][
               'opName'] == op.op_name
    assert pipeline_query.data['pipelines']['edges'][0]['node']['links']['edges'][0]['node']['downstreamOp'] is None
    pipeline_id = pipeline_query.data['pipelines']['edges'][0]['node']['id']

    # direct query
    direct_query = metadata_schema.execute(single_pipeline_query_str, variable_values={'id': pipeline_id})

    assert not direct_query.errors
    assert direct_query.data['pipeline']['pipelineName'] == pipeline.pipeline_name
    assert len(direct_query.data['pipeline']['links']['edges']) == 1
    assert direct_query.data['pipeline']['links']['edges'][0]['node']['upstreamOp']['opName'] == op.op_name
    assert direct_query.data['pipeline']['links']['edges'][0]['node']['downstreamOp'] is None
    clean_db(session_cls, models)


def test_pipeline_links_multiple_links(metadata_schema, pipeline, complex_links, complex_ops, models, session_cls):
    # querying all the pipelines

    pipeline_query = metadata_schema.execute(pipelines_query_str)
    assert not pipeline_query.errors
    assert len(pipeline_query.data['pipelines']['edges']) == 1
    assert pipeline_query.data['pipelines']['edges'][0]['node']['pipelineName'] == pipeline.pipeline_name
    assert len(pipeline_query.data['pipelines']['edges'][0]['node']['links']['edges']) == 4
    pipeline_id = pipeline_query.data['pipelines']['edges'][0]['node']['id']

    def do_link_test(links, complex_ops):
        links = {
            (link['node']['upstreamOp']['opName'], link['node']['downstreamOp']['opName'])
            for link in links
        }
        assert links == {
            (complex_ops[1].op_name, complex_ops[2].op_name),
            (complex_ops[1].op_name, complex_ops[3].op_name),
            (complex_ops[2].op_name, complex_ops[4].op_name),
            (complex_ops[3].op_name, complex_ops[4].op_name),
        }

    do_link_test(pipeline_query.data['pipelines']['edges'][0]['node']['links']['edges'], complex_ops)
    # direct query
    direct_query = metadata_schema.execute(single_pipeline_query_str, variable_values={'id': pipeline_id})

    assert not direct_query.errors
    assert direct_query.data['pipeline']['pipelineName'] == pipeline.pipeline_name
    assert len(direct_query.data['pipeline']['links']['edges']) == 4
    do_link_test(direct_query.data['pipeline']['links']['edges'], complex_ops)
    clean_db(session_cls, models)


def test_pipeline_validated_links_multiple_versions(metadata_schema, complex_ops, pipeline, complex_versions,
                                                    complex_validated_links, session_cls, models):

    filtered_op = metadata_schema.execute("""
query getOp($name: String!){
  ops (name: $name) {
    edges {
      node {
        id
        opName
        upstreamValidatedLinks {
          edges {
            node {
              id
              upstreamOp {opName}
              upstreamVersion {
                majorHash
                minorHash
                patchHash
                majorVersionNumber
                minorVersionNumber
                patchVersionNumber
              }
            }
          }
        }
      }
    }
  }
}
    """, variable_values={'name': complex_ops[4].op_name})
    assert not filtered_op.errors
    assert len(filtered_op.data['ops']['edges']) == 1
    assert filtered_op.data['ops']['edges'][0]['node']['opName'] == complex_ops[4].op_name
    assert len(filtered_op.data['ops']['edges'][0]['node']['upstreamValidatedLinks']['edges']) == 2
    validated_links = {
        link['node']['upstreamOp']['opName']: link['node']
        for link in filtered_op.data['ops']['edges'][0]['node']['upstreamValidatedLinks']['edges']
    }
    assert set(validated_links) == {complex_ops[2].op_name, complex_ops[3].op_name}

    def do_upstream_version_test(validated_links, op_n, complex_versions):
        assert validated_links[complex_ops[op_n].op_name]['upstreamVersion']['minorHash'] == \
               complex_versions[op_n].minor_hash
        assert validated_links[complex_ops[op_n].op_name]['upstreamVersion']['majorHash'] == \
               complex_versions[op_n].major_hash
        assert validated_links[complex_ops[op_n].op_name]['upstreamVersion']['patchHash'] == \
               complex_versions[op_n].patch_hash
        assert validated_links[complex_ops[op_n].op_name]['upstreamVersion']['majorVersionNumber'] == \
               complex_versions[op_n].major_version_number
        assert validated_links[complex_ops[op_n].op_name]['upstreamVersion']['minorVersionNumber'] == \
               complex_versions[op_n].minor_version_number
        assert validated_links[complex_ops[op_n].op_name]['upstreamVersion']['patchVersionNumber'] == complex_versions[
            op_n].patch_version_number

    do_upstream_version_test(validated_links, 2, complex_versions)
    do_upstream_version_test(validated_links, 3, complex_versions)
    clean_db(session_cls, models)
