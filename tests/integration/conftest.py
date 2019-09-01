import pytest

from chariots import Pipeline
from chariots.nodes import Node
from chariots.nodes import ReservedNodes


@pytest.fixture
def pipe_generator(savable_op_generator, Range10):

    def inner(counter_step):
        pipe = Pipeline([
            Node(Range10(), output_nodes="my_list"),
            Node(savable_op_generator(counter_step)(), input_nodes=["my_list"],
                 output_nodes=ReservedNodes.pipeline_output)
        ], name="my_pipe")
        return pipe
    return inner


@pytest.fixture
def enchrined_pipelines_generator(NotOp, pipe_generator):
    def inner(counter_step):
        pipe1 = pipe_generator(counter_step=counter_step)
        pipe = Pipeline([
            Node(pipe1, output_nodes="first_pipe"),
            Node(NotOp(), input_nodes=["first_pipe"], output_nodes=ReservedNodes.pipeline_output)
        ], name="outer_pipe")
        return pipe
    return inner
