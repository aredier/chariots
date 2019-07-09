import pytest

from chariots.core.pipelines import Pipeline, SequentialRunner, ReservedNodes
from chariots.core.nodes import  Node
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


@pytest.fixture
def enchrined_pipelines_generator(NotOp, pipe_generator):
    def inner(counter_step):
        pipe1 = pipe_generator(counter_step=counter_step)
        pipe1.set_pipeline_name("first_pipe")
        pipe = Pipeline([
            Node(pipe1, output_node="first_pipe"),
            Node(NotOp(), input_nodes=["first_pipe"], output_node=ReservedNodes.pipeline_output)
        ])
        pipe.set_pipeline_name("complete_pipe")
        return pipe
    return inner


def test_savable_pipeline(pipe_generator, tmpdir):

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


def test_savable_pipeline_wrong_version(pipe_generator, tmpdir):

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

    pipe_load = pipe_generator(counter_step=2)
    pipe_load.set_pipeline_name("my_pipe")
    with pytest.raises(ValueError):
        pipe_load.load(saver)

    res = pipe_load(SequentialRunner())
    assert len(res) == 10
    assert res == [not i % 2 for i in range(10)]


def test_saving_with_pipe_as_op(enchrined_pipelines_generator, NotOp, tmpdir):
    pipe = enchrined_pipelines_generator(counter_step=1)
    res = pipe(SequentialRunner())

    assert len(res) == 10
    assert res == [bool(i % 1) for i in range(10)]

    res = pipe(SequentialRunner())
    assert len(res) == 10
    assert res == [bool(i % 2) for i in range(10)]

    saver = FileSaver(tmpdir)
    pipe.set_pipeline_name("my_pipe")
    pipe.save(saver)

    del pipe

    pipe_load = enchrined_pipelines_generator(counter_step=1)
    pipe_load.set_pipeline_name("my_pipe")
    pipe_load.load(saver)

    res = pipe_load(SequentialRunner())
    assert len(res) == 10
    assert res == [bool(i % 3) for i in range(10)]

