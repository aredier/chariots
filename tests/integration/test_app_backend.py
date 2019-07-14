import json
import os

from chariots.backend.app import Chariot, PipelineResponse
from chariots.core.pipelines import Pipeline, ReservedNodes
from chariots.core.nodes import Node, DataLoadingNode, DataSavingNode
from chariots.core.saving import JSONSerializer
from chariots.core.versioning import Version


def post_app(client, route, data=None):
    response = client.post(route, data=json.dumps(data), content_type='application/json')
    return json.loads(response.data)


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
    test_client = app.test_client()

    response_json = post_app(test_client, "/pipelines/outer_pipe/main")
    assert "pipeline_output" in response_json
    assert "versions" in response_json
    response = PipelineResponse(response_json["pipeline_output"],
                                {pipe.node_for_name[node_name]: Version.parse(version_str)
                                 for node_name, version_str in response_json["versions"].items()})
    assert response.value == [i % 2 for i in range(10)]

    response_json = post_app(test_client, "/pipelines/inner_pipe/main")
    response = PipelineResponse(response_json["pipeline_output"],
                                {pipe1.node_for_name[node_name]: Version.parse(version_str)
                                 for node_name, version_str in response_json["versions"].items()})
    assert response.value == [not i % 2 for i in range(10)]


def test_app_response_with_input(Range10, IsPair, NotOp, tmpdir):
    pipe1 = Pipeline([
        Node(IsPair(), input_nodes=["__pipeline_input__"], output_node="__pipeline_output__")
    ], name="inner_pipe")

    app = Chariot([pipe1], path=tmpdir, import_name="some_app")
    test_client = app.test_client()

    response_json = post_app(test_client, "/pipelines/inner_pipe/main", data={"pipeline_input": list(range(20))})
    response = PipelineResponse(response_json["pipeline_output"],
                                {pipe1.node_for_name[node_name]: Version.parse(version_str)
                                 for node_name, version_str in response_json["versions"].items()})
    assert response.value == [not i % 2 for i in range(20)]


def test_app_with_data_nodes(NotOp, tmpdir):
    input_path = "in.json"
    output_path = "out.json"

    with open(os.path.join(tmpdir, input_path), "w") as file:
        json.dump(list(range(10)), file)

    in_node = DataLoadingNode(JSONSerializer(), input_path, output_node="data_in")
    out_node = DataSavingNode(JSONSerializer(), output_path, input_nodes=["data_trans"])

    pipe = Pipeline([
        in_node,
        Node(NotOp(), input_nodes=["data_in"], output_node="data_trans"),
        out_node
    ], name="my_pipe")

    app = Chariot([pipe], path=tmpdir, import_name="some_app")
    test_client = app.test_client()

    response_json = post_app(test_client, "/pipelines/my_pipe/main")
    _ = PipelineResponse(response_json["pipeline_output"],
                                {pipe.node_for_name[node_name]: Version.parse(version_str)
                                 for node_name, version_str in response_json["versions"].items()})

    with open(os.path.join(tmpdir, output_path), "r") as file:
        res = json.load(file)

    assert len(res) == 10
    assert res == [True] + [False] * 9

