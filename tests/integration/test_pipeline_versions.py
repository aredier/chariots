from chariots.core.pipelines import Pipeline
from chariots.core.nodes import Node


def test_pipe_version_op_change(savable_op_generator, Range10):
    range_node = Node(Range10(), output_node="my_list")
    op_node = Node(savable_op_generator(counter_step=1), input_nodes=[range_node],
                   output_node="__pipeline_output__")
    pipe = Pipeline([
        range_node,
        op_node,
    ], name="my_pipe")

    versions = pipe.get_pipeline_versions()
    op_version = versions[op_node]

    range_node = Node(Range10(), output_node="my_list")
    op_node = Node(savable_op_generator(counter_step=2), input_nodes=[range_node],
                   output_node="__pipeline_output__")
    pipe = Pipeline([
        range_node,
        op_node,
    ], name="my_pipe")

    versions = pipe.get_pipeline_versions()
    op_version_2 = versions[op_node]
    assert op_version < op_version_2
    assert op_version.major != op_version_2.major
    assert op_version.minor == op_version_2.minor
    assert op_version.patch == op_version_2.patch


def test_pipe_version_ancestry_change(NotOp, savable_op_generator, Range10):
    range_node = Node(Range10(), output_node="my_list")
    op_node = Node(savable_op_generator(counter_step=1), input_nodes=[range_node],
                   output_node="__pipeline_output__")
    pipe = Pipeline([
        range_node,
        op_node,
    ], name="my_pipe")

    versions = pipe.get_pipeline_versions()
    op_version = versions[op_node]

    range_node = Node(Range10())
    not_node = Node(NotOp(), input_nodes=[range_node])
    op_node = Node(savable_op_generator(counter_step=1), input_nodes=[not_node],
                   output_node="__pipeline_output__")
    pipe = Pipeline([
        range_node,
        not_node,
        op_node,
    ], name="my_pipe")

    versions = pipe.get_pipeline_versions()
    op_version_2 = versions[op_node]
    assert op_version < op_version_2
    assert op_version.major != op_version_2.major
    assert op_version.minor != op_version_2.minor
    assert op_version.patch != op_version_2.patch


def test_forget_pipe_version_in_pipe(NotOp, savable_op_generator, Range10):
    range_node = Node(Range10())
    op_node = Node(savable_op_generator(counter_step=1), input_nodes=["__pipeline_input__"],
                   output_node="__pipeline_output__")
    inner_pipe = Pipeline([
        op_node
    ], name="inner_pipe")
    inner_pipe_node = Node(inner_pipe,  input_nodes=[range_node], output_node="__pipeline_output__")
    pipe = Pipeline([
        range_node,
        inner_pipe_node
    ], name="outer_pipe")

    versions = inner_pipe.get_pipeline_versions()
    op_version = versions[op_node]
    versions = pipe.get_pipeline_versions()
    pipe_version = versions[inner_pipe_node]

    range_node = Node(Range10())
    op_node = Node(savable_op_generator(counter_step=1), input_nodes=["__pipeline_input__"],
                   output_node="__pipeline_output__")
    inner_pipe = Pipeline([
        op_node
    ], name="inner_pipe")
    not_node = Node(NotOp(), input_nodes=[range_node])
    inner_pipe_node = Node(inner_pipe, input_nodes=[not_node], output_node="__pipeline_output__")
    pipe = Pipeline([
        range_node,
        not_node,
        inner_pipe_node
    ], name="outer_pipe")

    versions = inner_pipe.get_pipeline_versions()
    op_version_2 = versions[op_node]
    versions = pipe.get_pipeline_versions()
    pipe_version_2 = versions[inner_pipe_node]
    assert pipe_version_2 > pipe_version
    assert op_version_2 == op_version
