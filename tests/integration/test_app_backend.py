import json
import os

from chariots.backend.app import Chariot
from chariots.backend.client import TestClient
from chariots.core.pipelines import Pipeline, ReservedNodes, SequentialRunner
from chariots.core.nodes import Node, DataLoadingNode, DataSavingNode
from chariots.core.saving import JSONSerializer


def test_app_response(Range10, IsPair, NotOp, tmpdir):
    pipe1 = Pipeline([
        Node(Range10(), output_node="my_list"),
        Node(IsPair(), input_nodes=["my_list"], output_node="__pipeline_output__")
    ], name="inner_pipe")

    pipe = Pipeline([
        Node(pipe1, output_node="og_pipe"),
        Node(NotOp(), input_nodes=["og_pipe"], output_node=ReservedNodes.pipeline_output)
    ], name="outer_pipe")

    app = Chariot([pipe1, pipe], path=tmpdir, import_name="some_app")
    test_client = TestClient(app)

    response = test_client.request(pipe)
    assert response.value == [i % 2 for i in range(10)]

    response = test_client.request(pipe1)
    assert response.value == [not i % 2 for i in range(10)]


def test_app_response_with_input(Range10, IsPair, NotOp, tmpdir):
    pipe1 = Pipeline([
        Node(IsPair(), input_nodes=["__pipeline_input__"], output_node="__pipeline_output__")
    ], name="inner_pipe")

    app = Chariot([pipe1], path=tmpdir, import_name="some_app")
    test_client = TestClient(app)

    response = test_client.request(pipe1, pipeline_input=list(range(20)))
    assert response.value == [not i % 2 for i in range(20)]


def test_app_with_data_nodes(NotOp, tmpdir):
    input_path = "in.json"
    output_path = "out.json"

    os.makedirs(os.path.join(tmpdir, "data"), exist_ok=True)
    with open(os.path.join(tmpdir, "data", input_path), "w") as file:
        json.dump(list(range(10)), file)

    in_node = DataLoadingNode(JSONSerializer(), input_path, output_node="data_in")
    out_node = DataSavingNode(JSONSerializer(), output_path, input_nodes=["data_trans"])

    pipe = Pipeline([
        in_node,
        Node(NotOp(), input_nodes=["data_in"], output_node="data_trans"),
        out_node
    ], name="my_pipe")

    app = Chariot([pipe], path=tmpdir, import_name="some_app")
    test_client = TestClient(app)
    test_client.request(pipe)

    with open(os.path.join(tmpdir, "data", output_path), "r") as file:
        res = json.load(file)

    assert len(res) == 10
    assert res == [True] + [False] * 9


def test_app_persistance(enchrined_pipelines_generator, NotOp, tmpdir):
    pipe = enchrined_pipelines_generator(counter_step=1)

    app = Chariot([pipe], path=tmpdir, import_name="some_app")
    test_client = TestClient(app)
    res = test_client.request(pipe)
    assert len(res.value) == 10
    assert res.value == [bool(i % 1) for i in range(10)]

    res = test_client.request(pipe)
    assert len(res.value) == 10
    assert res.value == [bool(i % 2) for i in range(10)]

    test_client.save_pipeline(pipe)
    test_client.load_pipeline(pipe)

    res = test_client.request(pipe)
    assert len(res.value) == 10
    assert res.value == [bool(i % 3) for i in range(10)]

    test_client.save_pipeline(pipe)
    del test_client
    del app
    del pipe

    pipe = enchrined_pipelines_generator(counter_step=1)
    app = Chariot([pipe], path=tmpdir, import_name="some_app")
    test_client = TestClient(app)

    test_client.load_pipeline(pipe)
    res = test_client.request(pipe)
    assert len(res.value) == 10
    assert res.value == [bool(i % 4) for i in range(10)]
