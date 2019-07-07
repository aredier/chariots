import json

from chariots.backend.app import Chariot, PipelineResponse
from chariots.core.pipelines import Pipeline, Node, ReservedNodes
from chariots.core.versioning import Version


def post_app(client, route, data=None):
    response = client.post(route, data=json.dumps(data), content_type='application/json')
    print(response)
    return json.loads(response.data)


def test_app_response(Range10, IsPair, NotOp):
    pipe1 = Pipeline([
        Node(Range10(), output_node="my_list"),
        Node(IsPair(), input_nodes=["my_list"], output_node="__pipeline_output__")
    ], name="inner_pipe")

    pipe = Pipeline([
        Node(pipe1, output_node="og_pipe"),
        Node(NotOp(), input_nodes=["og_pipe"], output_node=ReservedNodes.pipeline_output)
    ], name="outer_pipe")

    app = Chariot([pipe1, pipe], import_name="some_app")
    test_client = app.test_client()

    response_json = post_app(test_client, "/pipes/outer_pipe")
    assert "pipeline_output" in response_json
    assert "versions" in response_json
    response = PipelineResponse(response_json["pipeline_output"],
                                {pipe.node_for_name[node_name]: Version.parse(version_str)
                                 for node_name, version_str in response_json["versions"].items()})
    assert response.value == [i % 2 for i in range(10)]

    response_json = post_app(test_client, "/pipes/inner_pipe")
    response = PipelineResponse(response_json["pipeline_output"],
                                {pipe1.node_for_name[node_name]: Version.parse(version_str)
                                 for node_name, version_str in response_json["versions"].items()})
    assert response.value == [not i % 2 for i in range(10)]


def test_app_response_with_input(Range10, IsPair, NotOp):
    pipe1 = Pipeline([
        Node(IsPair(), input_nodes=["__pipeline_input__"], output_node="__pipeline_output__")
    ], name="inner_pipe")

    app = Chariot([pipe1], import_name="some_app")
    test_client = app.test_client()

    response_json = post_app(test_client, "/pipes/inner_pipe", data={"pipeline_input": list(range(20))})
    response = PipelineResponse(response_json["pipeline_output"],
                                {pipe1.node_for_name[node_name]: Version.parse(version_str)
                                 for node_name, version_str in response_json["versions"].items()})
    assert response.value == [not i % 2 for i in range(20)]
