import json
import os

from chariots import OpStore, Pipeline
from chariots.nodes import Node
from chariots.nodes import DataSavingNode
from chariots.nodes import DataLoadingNode
from chariots.runners import SequentialRunner
from chariots.savers import FileSaver
from chariots.serializers import JSONSerializer


def test_savable_pipeline(pipe_generator, tmpdir):

    op_store = OpStore(FileSaver(str(tmpdir)))
    pipe = pipe_generator(counter_step=1)
    pipe.load(op_store)
    runner = SequentialRunner()

    res = runner.run(pipe)
    assert len(res) == 10
    assert res == [not i % 1 for i in range(10)]

    res = runner.run(pipe)
    assert len(res) == 10
    assert res == [not i % 2 for i in range(10)]

    pipe.save(op_store)

    del pipe

    pipe_load = pipe_generator(counter_step=1)
    pipe_load.load(op_store)

    res = runner.run(pipe_load)
    assert len(res) == 10
    assert res == [not i % 3 for i in range(10)]


def test_savable_pipeline_new_version(pipe_generator, tmpdir):
    """
    here we simulate a version change by the user, the loading of the pipeline shouldn't take the
    saved version but use the new code
    """

    op_store = OpStore(FileSaver(str(tmpdir)))
    pipe = pipe_generator(counter_step=1)
    pipe.load(op_store)
    runner = SequentialRunner()

    res = runner.run(pipe)
    assert len(res) == 10
    assert res == [not i % 1 for i in range(10)]

    res = runner.run(pipe)
    assert len(res) == 10
    assert res == [not i % 2 for i in range(10)]

    pipe.save(op_store)

    del pipe

    pipe_load = pipe_generator(counter_step=2)
    pipe_load.load(op_store)

    res = runner.run(pipe_load)
    assert len(res) == 10
    assert res == [not i % 2 for i in range(10)]

    res = runner.run(pipe_load)
    assert len(res) == 10
    assert res == [not i % 4 for i in range(10)]


def test_saving_with_pipe_as_op(enchrined_pipelines_generator, NotOp, tmpdir):
    pipe = enchrined_pipelines_generator(counter_step=1)
    runner = SequentialRunner()
    res = runner.run(pipe)

    assert len(res) == 10
    assert res == [bool(i % 1) for i in range(10)]

    res = runner.run(pipe)
    assert len(res) == 10
    assert res == [bool(i % 2) for i in range(10)]

    op_store = OpStore(FileSaver(str(tmpdir)))
    pipe.save(op_store)

    del pipe

    pipe_load = enchrined_pipelines_generator(counter_step=1)
    pipe_load.load(op_store)

    res = runner.run(pipe_load)
    assert len(res) == 10
    assert res == [bool(i % 3) for i in range(10)]


def test_data_ops(tmpdir, NotOp):

    input_path = "in.json"
    output_path = "out.json"

    os.makedirs(os.path.join(str(tmpdir), "data"), exist_ok=True)
    with open(os.path.join(str(tmpdir), "data", input_path), "w") as file:
        json.dump(list(range(10)), file)

    saver = FileSaver(str(tmpdir))
    in_node = DataLoadingNode(JSONSerializer(), input_path, output_nodes="data_in")
    out_node = DataSavingNode(JSONSerializer(), output_path, input_nodes=["data_trans"])
    in_node.attach_saver(saver)
    out_node.attach_saver(saver)

    pipe = Pipeline([
        in_node,
        Node(NotOp(), input_nodes=["data_in"], output_nodes="data_trans"),
        out_node
    ], name="my_pipe")
    runner = SequentialRunner()

    runner.run(pipe)

    with open(os.path.join(str(tmpdir), "data", output_path), "r") as file:
        res = json.load(file)

    assert len(res) == 10
    assert res == [True] + [False] * 9
