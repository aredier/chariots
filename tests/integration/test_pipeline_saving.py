import json
import os

import pytest

from chariots.core.pipelines import Pipeline, SequentialRunner, ReservedNodes
from chariots.core.nodes import Node, DataLoadingNode, DataSavingNode
from chariots.core.saving import FileSaver, JSONSerializer


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


def test_savable_pipeline(pipe_generator, tmpdir):

    pipe = pipe_generator(counter_step=1)

    res = pipe(SequentialRunner())
    assert len(res) == 10
    assert res == [not i % 1 for i in range(10)]

    res = pipe(SequentialRunner())
    assert len(res) == 10
    assert res == [not i % 2 for i in range(10)]

    saver = FileSaver(tmpdir)
    pipe.save(saver)

    del pipe

    pipe_load = pipe_generator(counter_step=1)
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
    pipe.save(saver)

    del pipe

    pipe_load = pipe_generator(counter_step=2)
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
    pipe.save(saver)

    del pipe

    pipe_load = enchrined_pipelines_generator(counter_step=1)
    pipe_load.load(saver)

    res = pipe_load(SequentialRunner())
    assert len(res) == 10
    assert res == [bool(i % 3) for i in range(10)]


def test_data_ops(tmpdir, NotOp):

    input_path = "in.json"
    output_path = "out.json"

    with open(os.path.join(tmpdir, input_path), "w") as file:
        json.dump(list(range(10)), file)

    saver = FileSaver(tmpdir)
    in_node = DataLoadingNode(JSONSerializer(), input_path, output_node="data_in")
    out_node = DataSavingNode(JSONSerializer(), output_path, input_nodes=["data_trans"])
    in_node.attach_saver(saver)
    out_node.attach_saver(saver)

    pipe = Pipeline([
        in_node,
        Node(NotOp(), input_nodes=["data_in"], output_node="data_trans"),
        out_node
    ], name="my_pipe")

    pipe(SequentialRunner())

    with open(os.path.join(tmpdir, output_path), "r") as file:
        res = json.load(file)

    assert len(res) == 10
    assert res == [True] + [False] * 9
