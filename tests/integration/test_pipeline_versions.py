from chariots import Pipeline
from chariots.nodes import Node


def test_pipe_version_op_change(savable_op_generator, Range10):
    range_node = Node(Range10(), output_nodes="my_list")
    op_node = Node(savable_op_generator(counter_step=1)(), input_nodes=[range_node],
                   output_nodes="__pipeline_output__")
    pipe = Pipeline([
        range_node,
        op_node,
    ], name="my_pipe")

    versions = pipe.get_pipeline_versions()
    op_version = versions[op_node]

    range_node = Node(Range10(), output_nodes="my_list")
    op_node = Node(savable_op_generator(counter_step=2)(), input_nodes=[range_node],
                   output_nodes="__pipeline_output__")
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
