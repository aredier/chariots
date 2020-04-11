"""module to test that the flask layer of the Chariots app works properly"""
from chariots.pipelines import Chariots, Pipeline
from chariots.pipelines.nodes import Node, ReservedNodes
from chariots.testing import TestClient


def test_app_response(Range10, IsPair, NotOp, tmpdir, opstore_func):  # pylint: disable=invalid-name
    """check basic response call behavior"""
    pipe1 = Pipeline([
        Node(Range10(), output_nodes='my_list'),
        Node(IsPair(), input_nodes=['my_list'], output_nodes='__pipeline_output__')
    ], name='inner_pipe')

    pipe = Pipeline([
        Node(pipe1, output_nodes='og_pipe'),
        Node(NotOp(), input_nodes=['og_pipe'], output_nodes=ReservedNodes.pipeline_output)
    ], name='outer_pipe')

    app = Chariots([pipe1, pipe], op_store_client=opstore_func(tmpdir), import_name='some_app')
    test_client = TestClient(app)

    response = test_client.call_pipeline(pipe)
    assert response.value == [i % 2 for i in range(10)]

    response = test_client.call_pipeline(pipe1)
    assert response.value == [not i % 2 for i in range(10)]


def test_app_response_with_input(IsPair, tmpdir, opstore_func):  # pylint: disable=invalid-name
    """check the app response to a pipeline call with an input"""
    pipe1 = Pipeline([
        Node(IsPair(), input_nodes=['__pipeline_input__'], output_nodes='__pipeline_output__')
    ], name='inner_pipe')

    app = Chariots([pipe1], op_store_client=opstore_func(tmpdir), import_name='some_app')
    test_client = TestClient(app)

    response = test_client.call_pipeline(pipe1, pipeline_input=list(range(20)))
    assert response.value == [not i % 2 for i in range(20)]


def test_app_persistance(enchrined_pipelines_generator, tmpdir, opstore_func):
    """test the app is able to persist and reload ops that have a state"""
    pipe = enchrined_pipelines_generator(counter_step=1)

    op_store_client = opstore_func(tmpdir)
    app = Chariots([pipe], op_store_client=op_store_client, import_name='some_app')
    test_client = TestClient(app)
    res = test_client.call_pipeline(pipe)
    assert len(res.value) == 10
    assert res.value == [bool(i % 1) for i in range(10)]

    res = test_client.call_pipeline(pipe)
    assert len(res.value) == 10
    assert res.value == [bool(i % 2) for i in range(10)]

    test_client.save_pipeline(pipe)
    test_client.load_pipeline(pipe)

    res = test_client.call_pipeline(pipe)
    assert len(res.value) == 10
    assert res.value == [bool(i % 3) for i in range(10)]

    test_client.save_pipeline(pipe)
    del test_client
    del app
    del pipe

    pipe = enchrined_pipelines_generator(counter_step=1)
    app = Chariots([pipe], op_store_client=op_store_client, import_name='some_app')
    test_client = TestClient(app)

    test_client.load_pipeline(pipe)
    res = test_client.call_pipeline(pipe)
    assert len(res.value) == 10
    assert res.value == [bool(i % 4) for i in range(10)]
