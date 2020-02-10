"""conftest module for the integration tests"""
import json
import os

import pytest

from chariots import Pipeline
from chariots.nodes import Node, DataLoadingNode, DataSavingNode
from chariots.nodes import ReservedNodes
from chariots.serializers import JSONSerializer


@pytest.fixture
def pipe_generator(savable_op_generator, Range10):  # pylint: disable=invalid-name
    """fixture that returns a function to generates a simple pipeline"""

    def inner(counter_step):
        pipe = Pipeline([
            Node(Range10(), output_nodes='my_list'),
            Node(savable_op_generator(counter_step)(), input_nodes=['my_list'],
                 output_nodes=ReservedNodes.pipeline_output)
        ], name='my_pipe')
        return pipe
    return inner


@pytest.fixture
def enchrined_pipelines_generator(
        NotOp, pipe_generator  # pylint: disable=redefined-outer-name
):  # pylint: disable=invalid-name
    """fixtures that returns a function to have enshrined pipelines (a pipeline as an op in another pipeline)"""
    def inner(counter_step):
        pipe1 = pipe_generator(counter_step=counter_step)
        pipe = Pipeline([
            Node(pipe1, output_nodes='first_pipe'),
            Node(NotOp(), input_nodes=['first_pipe'], output_nodes=ReservedNodes.pipeline_output)
        ], name='outer_pipe')
        return pipe
    return inner


@pytest.fixture
def data_nodes_paths(tmpdir):
    """fixture that prepares the needed files for data nodes test"""
    input_path = 'in.json'
    output_path = 'out.json'

    os.makedirs(os.path.join(str(tmpdir), 'data'), exist_ok=True)
    with open(os.path.join(str(tmpdir), 'data', input_path), 'w') as file:
        json.dump(list(range(10)), file)
    return input_path, output_path


@pytest.fixture
def data_nodes_pipeline(data_nodes_paths, NotOp):  # pylint: disable=invalid-name, redefined-outer-name
    """basic pipeline including data nodes for tests"""

    input_path, output_path = data_nodes_paths

    in_node = DataLoadingNode(JSONSerializer(), input_path, output_nodes='data_in')
    out_node = DataSavingNode(JSONSerializer(), output_path, input_nodes=['data_trans'])

    pipe = Pipeline([
        in_node,
        Node(NotOp(), input_nodes=['data_in'], output_nodes='data_trans'),
        out_node
    ], name='my_pipe')
    return pipe, in_node, out_node
