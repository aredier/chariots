"""module to test the saving and loading behavior"""
import json
import os

from chariots import OpStore
from chariots.runners import SequentialRunner
from chariots.savers import FileSaver


def test_savable_pipeline(pipe_generator, tmpdir, opstore_func):
    """test basic save and load"""

    op_store_client = opstore_func(tmpdir)
    pipe = pipe_generator(counter_step=1)
    pipe.load(op_store_client)
    runner = SequentialRunner()

    res = runner.run(pipe)
    assert len(res) == 10
    assert res == [not i % 1 for i in range(10)]

    res = runner.run(pipe)
    assert len(res) == 10
    assert res == [not i % 2 for i in range(10)]

    pipe.save(op_store_client)

    del pipe

    pipe_load = pipe_generator(counter_step=1)
    pipe_load.load(op_store_client)

    res = runner.run(pipe_load)
    assert len(res) == 10
    assert res == [not i % 3 for i in range(10)]


def test_savable_pipeline_new_version(pipe_generator, tmpdir, opstore_func):
    """
    here we simulate a version change by the user, the loading of the pipeline shouldn't take the
    saved version but use the new code
    """

    op_store_client = opstore_func(tmpdir)
    pipe = pipe_generator(counter_step=1)
    pipe.load(op_store_client)
    runner = SequentialRunner()

    res = runner.run(pipe)
    assert len(res) == 10
    assert res == [not i % 1 for i in range(10)]

    res = runner.run(pipe)
    assert len(res) == 10
    assert res == [not i % 2 for i in range(10)]

    pipe.save(op_store_client)

    del pipe

    pipe_load = pipe_generator(counter_step=2)
    pipe_load.load(op_store_client)

    res = runner.run(pipe_load)
    assert len(res) == 10
    assert res == [not i % 2 for i in range(10)]

    res = runner.run(pipe_load)
    assert len(res) == 10
    assert res == [not i % 4 for i in range(10)]


def test_saving_with_pipe_as_op(enchrined_pipelines_generator, tmpdir, opstore_func):
    """test saving when one of the ops of the saved pipeline is a pipeline itself"""
    pipe = enchrined_pipelines_generator(counter_step=1)
    runner = SequentialRunner()
    res = runner.run(pipe)

    assert len(res) == 10
    assert res == [bool(i % 1) for i in range(10)]

    res = runner.run(pipe)
    assert len(res) == 10
    assert res == [bool(i % 2) for i in range(10)]

    op_store_client = opstore_func(tmpdir)
    pipe.save(op_store_client)

    del pipe

    pipe_load = enchrined_pipelines_generator(counter_step=1)
    pipe_load.load(op_store_client)

    res = runner.run(pipe_load)
    assert len(res) == 10
    assert res == [bool(i % 3) for i in range(10)]


def test_data_ops(tmpdir, data_nodes_paths, data_nodes_pipeline):  # pylint: disable=invalid-name
    """test the saving behavior of data ops"""

    _, output_path = data_nodes_paths
    pipe, in_node, out_node = data_nodes_pipeline

    saver = FileSaver(str(tmpdir))
    in_node.attach_saver(saver)
    out_node.attach_saver(saver)

    runner = SequentialRunner()

    runner.run(pipe)

    with open(os.path.join(str(tmpdir), 'data', output_path), 'r') as file:
        res = json.load(file)

    assert len(res) == 10
    assert res == [True] + [False] * 9
