import pytest

from chariots.core.ops import AbstractOp
from chariots.core.pipelines import Node, Pipeline, ReservedNodes


@pytest.fixture
def Range10():

    class Inner(AbstractOp):

        def __call__(self, *args, **kwargs):
            return list(range(10))
    return Inner


@pytest.fixture
def IsPair():

    class Inner(AbstractOp):

        def __call__(self, data):
            return [not i % 2 for i in data]
    return Inner


def test_pipeline_simple(Range10, IsPair):
    pipe = Pipeline([
        Node(Range10(), output_node="my_list"),
        Node(IsPair(), input_nodes=["my_list"], output_node="__pipeline_output__")
    ])
    res = pipe()
    assert len(res) == 10
    assert res == [not i % 2 for i in range(10)]


def test_pipeline_with_defined_nodes(Range10, IsPair):
    range_node = Node(Range10())
    pair_node = Node(IsPair(), input_nodes=[range_node], output_node=ReservedNodes.pipeline_output)
    pipe = Pipeline([
        range_node,
        pair_node,
    ])
    res = pipe()
    assert len(res) == 10
    assert res == [not i % 2 for i in range(10)]


def test_pipeline_as_op(Range10, IsPair):
    pipe1 = Pipeline([
        Node(Range10(), output_node="my_list"),
        Node(IsPair(), input_nodes=["my_list"], output_node="__pipeline_output__")
    ])

    class NotOp(AbstractOp):

        def __call__(self, input):
            return [not i for i in input]
    pipe = Pipeline([
        Node(pipe1, output_node="og_pipe"),
        Node(NotOp(), input_nodes=["og_pipe"], output_node=ReservedNodes.pipeline_output)
    ])
    res = pipe()
    assert len(res) == 10
    assert res == [i % 2 for i in range(10)]
