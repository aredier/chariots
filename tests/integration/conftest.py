import pytest

from chariots.core.nodes import Node
from chariots.core.pipelines import Pipeline, ReservedNodes


@pytest.fixture
def pipe_generator(savable_op_generator, Range10):

    def inner(counter_step):
        pipe = Pipeline([
            Node(Range10(), output_node="my_list"),
            Node(savable_op_generator(counter_step)(), input_nodes=["my_list"],
                 output_node=ReservedNodes.pipeline_output)
        ], name="my_pipe")
        return pipe
    return inner


@pytest.fixture
def enchrined_pipelines_generator(NotOp, pipe_generator):
    def inner(counter_step):
        pipe1 = pipe_generator(counter_step=counter_step)
        pipe = Pipeline([
            Node(pipe1, output_node="first_pipe"),
            Node(NotOp(), input_nodes=["first_pipe"], output_node=ReservedNodes.pipeline_output)
        ], name="outer_pipe")
        return pipe
    return inner
