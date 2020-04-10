"""
A node represents a step in a Pipeline. It is linked to one or several inputs and can produce one or several
ouptuts:

.. testsetup::

    >>> from chariots import Pipeline
    >>> from chariots.nodes import Node
    >>> from chariots._helpers.doc_utils import IrisFullDataSet, PCAOp, MLMode, LogisticOp

.. doctest::

    >>> train_logistics = Pipeline([
    ...     Node(IrisFullDataSet(), output_nodes=["x", "y"]),
    ...     Node(PCAOp(MLMode.FIT_PREDICT), input_nodes=["x"], output_nodes="x_transformed"),
    ...     Node(LogisticOp(MLMode.FIT), input_nodes=["x_transformed", "y"])
    ... ], 'train_logistics')

you can also link the first and/or the last node of your pipeline  to the pipeline input and output:

.. doctest::

    >>> pred = Pipeline([
    ...     Node(IrisFullDataSet(),input_nodes=['__pipeline_input__'], output_nodes=["x"]),
    ...     Node(PCAOp(MLMode.PREDICT), input_nodes=["x"], output_nodes="x_transformed"),
    ...     Node(LogisticOp(MLMode.PREDICT), input_nodes=["x_transformed"], output_nodes=['__pipeline_output__'])
    ... ], 'pred')
"""
from ._base_nodes import ReservedNodes, BaseNode

from ._node import Node


__all__ = [
    'Node',
    'ReservedNodes',
    'BaseNode',
]
