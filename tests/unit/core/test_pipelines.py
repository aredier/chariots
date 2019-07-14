import pytest

from chariots.core.ops import AbstractOp
from chariots.core.pipelines import Pipeline, SequentialRunner, ReservedNodes
from chariots.core.nodes import Node


@pytest.fixture
def Sum():

    class Inner(AbstractOp):

        def __call__(self, left, right):
            return [l + r for l, r in zip(left, right)]
    return Inner


def test_pipeline_simple(Range10, IsPair):
    pipe = Pipeline([
        Node(Range10(), output_node="my_list"),
        Node(IsPair(), input_nodes=["my_list"], output_node="__pipeline_output__")
    ], name="my_pipe")
    res = pipe(SequentialRunner())
    assert len(res) == 10
    assert res == [not i % 2 for i in range(10)]


def test_pipeline_with_defined_nodes(Range10, IsPair):
    range_node = Node(Range10())
    pair_node = Node(IsPair(), input_nodes=[range_node], output_node=ReservedNodes.pipeline_output)
    pipe = Pipeline([
        range_node,
        pair_node,
    ], name="my_pipe")
    res = pipe(SequentialRunner())
    assert len(res) == 10
    assert res == [not i % 2 for i in range(10)]


def test_pipeline_as_op(Range10, IsPair, NotOp):
    pipe1 = Pipeline([
        Node(Range10(), output_node="my_list"),
        Node(IsPair(), input_nodes=["my_list"], output_node="__pipeline_output__")
    ], name="my_pipe")


    pipe = Pipeline([
        Node(pipe1, output_node="og_pipe"),
        Node(NotOp(), input_nodes=["og_pipe"], output_node=ReservedNodes.pipeline_output)
    ], name="my_pipe")
    res = pipe(SequentialRunner())
    assert len(res) == 10
    assert res == [i % 2 for i in range(10)]


def test_multiple_input(Range10, Sum):
    pipe = Pipeline([
        Node(Range10(), output_node="left"),
        Node(Range10(), output_node="right"),
        Node(Sum(), input_nodes=["left", "right"], output_node="__pipeline_output__"),
    ], name="my_pipe")

    res = pipe(SequentialRunner())
    assert len(res) == 10
    assert res == [2 * i for i in range(10)]


