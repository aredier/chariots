"""module to test the behavior of the `Pipeline` class"""
import pytest

from chariots.base import BaseOp
from chariots.nodes import ReservedNodes
from chariots.runners import SequentialRunner
from chariots import Pipeline
from chariots.nodes import Node


@pytest.fixture
def Sum():  # pylint: disable=invalid-name
    """Op fixture that sums each element of its inputs"""

    class Inner(BaseOp):
        """Op of the fixture"""

        def execute(self, left, right):  # pylint: disable=arguments-differ
            return [l + r for l, r in zip(left, right)]
    return Inner


@pytest.fixture
def SplitOnes():  # pylint: disable=invalid-name
    """Op fixture that returns to list with a single `1` in each list"""

    class SplitOnesInner(BaseOp):
        """Op of the fixture"""

        def execute(self):  # pylint: disable=arguments-differ
            return [1], [1]

    return SplitOnesInner


def test_pipeline_simple(Range10, IsPair):  # pylint: disable=invalid-name
    """test the most basic pipeline possible"""
    runner = SequentialRunner()
    pipe = Pipeline([
        Node(Range10(), output_nodes='my_list'),
        Node(IsPair(), input_nodes=['my_list'], output_nodes='__pipeline_output__')
    ], name='my_pipe')

    res = runner.run(pipe)
    assert len(res) == 10
    assert res == [not i % 2 for i in range(10)]


def test_pipeline_with_defined_nodes(Range10, IsPair):  # pylint: disable=invalid-name
    """tests an op when node object are called direcly (instead of string refences)"""
    runner = SequentialRunner()
    range_node = Node(Range10())
    pair_node = Node(IsPair(), input_nodes=[range_node], output_nodes=ReservedNodes.pipeline_output)
    pipe = Pipeline([
        range_node,
        pair_node,
    ], name='my_pipe')

    res = runner.run(pipe)
    assert len(res) == 10
    assert res == [not i % 2 for i in range(10)]


def test_pipeline_as_op(Range10, IsPair, NotOp):  # pylint: disable=invalid-name
    """tests pipeline when one a pipeline is used as an op"""
    runner = SequentialRunner()
    pipe1 = Pipeline([
        Node(Range10(), output_nodes='my_list'),
        Node(IsPair(), input_nodes=['my_list'], output_nodes='__pipeline_output__')
    ], name='my_pipe')

    pipe = Pipeline([
        Node(pipe1, output_nodes='og_pipe'),
        Node(NotOp(), input_nodes=['og_pipe'], output_nodes=ReservedNodes.pipeline_output)
    ], name='my_pipe')

    res = runner.run(pipe)
    assert len(res) == 10
    assert res == [i % 2 for i in range(10)]


def test_multiple_input(Range10, Sum):  # pylint: disable=invalid-name, redefined-outer-name
    """test a pipeline where one of the ops thakes two other ops' output as input"""
    runner = SequentialRunner()
    pipe = Pipeline([
        Node(Range10(), output_nodes='left'),
        Node(Range10(), output_nodes='right'),
        Node(Sum(), input_nodes=['left', 'right'], output_nodes='__pipeline_output__'),
    ], name='my_pipe')

    res = runner.run(pipe)
    assert len(res) == 10
    assert res == [2 * i for i in range(10)]


def test_splited_outputs(SplitOnes, AddOne, Sum):  # pylint: disable=invalid-name, redefined-outer-name
    """tests an op that splits it's outputs (to be reused by different ops as input)"""
    runner = SequentialRunner()
    pipe = Pipeline([
        Node(SplitOnes(), output_nodes=['left', 'right']),
        Node(AddOne(), input_nodes=['left'], output_nodes=['new_left']),
        Node(AddOne(), input_nodes=['right'], output_nodes=['new_right']),
        Node(Sum(), input_nodes=['new_left', 'new_right'], output_nodes='__pipeline_output__')
    ], 'splited_pipeline')

    res = runner.run(pipe)
    assert len(res) == 1
    assert res == [4]
