import pytest

from chariots.core.pipelines import Pipeline, Node, ReservedNodes, SequentialRunner
from chariots.core.saving import FileSaver


@pytest.fixture
def pipe_generator(savable_op_generator, Range10):

    def inner(counter_step):
        pipe = Pipeline([
            Node(Range10(), output_node="my_list"),
            Node(savable_op_generator(counter_step)(), input_nodes=["my_list"],
                 output_node=ReservedNodes.pipeline_output)
        ])
        return pipe
    return inner


def test_savable_piepline(pipe_generator, tmpdir):

    pipe = pipe_generator(counter_step=1)

    res = pipe(SequentialRunner())
    assert len(res) == 10
    assert res == [not i % 1 for i in range(10)]

    res = pipe(SequentialRunner())
    assert len(res) == 10
    assert res == [not i % 2 for i in range(10)]

    saver = FileSaver(tmpdir)
    pipe.set_pipeline_name("my_pipe")
    pipe.save(saver)

    del pipe

    pipe_load = pipe_generator(counter_step=1)
    pipe_load.set_pipeline_name("my_pipe")
    pipe_load.load(saver)

    res = pipe_load(SequentialRunner())
    assert len(res) == 10
    assert res == [not i % 3 for i in range(10)]
