"""op callbacks module"""
from typing import List, Any

from .. import ops

class OpCallBack:
    """
    an op callback is used to perform specific instructions at certain points around the operation's
    execution

    to create your own op callback, you need to override either the `before_execution` or the `after_execution` method (
    or both)

    .. testsetup::

        >>> from chariots.pipelines import Pipeline
        >>> from chariots.pipelines.callbacks import OpCallBack
        >>> from chariots.pipelines.nodes import Node
        >>> from chariots.pipelines.runners import SequentialRunner
        >>> from chariots._helpers.doc_utils import IsOddOp, AddOneOp

        >>> runner = SequentialRunner()

    .. doctest::

        >>> class PrintOpName(OpCallBack):
        ...
        ...     def before_execution(self, op: "base.BaseOp", args: List[Any]):
        ...         print('{} called with {}'.format(op.name, args))

    .. doctest::

        >>> is_even_pipeline = Pipeline([
        ...     Node(AddOneOp(), input_nodes=['__pipeline_input__'], output_nodes='modified'),
        ...     Node(IsOddOp(op_callbacks=[PrintOpName()]), input_nodes=['modified'],
        ...          output_nodes=['__pipeline_output__'])
        ... ], 'simple_pipeline')
        >>> runner.run(is_even_pipeline, 3)
        isoddop called with [4]
        False
    """

    def before_execution(self, callback_op: 'ops.BaseOp', args: List[Any]):
        """
        called before the operation is executed (and before the operation's `before_execution`'s method).

        :param callback_op: the operation that is going to be executed
        :param args: the list of arguments that are going to be passed to the operation. DO NOT MODIFY those references
                     as this might cause some undefined behavior
        """

    def after_execution(self, callback_op: 'ops.BaseOp', args: List[Any], output: Any):
        """
        called after the operation has been executed (and after it's `after_execution`'s method).

        :param callback_op: the operation that was executed
        :param args: the arguments that were passed to the op
        :param output: the output the op produced. DO NOT MODIFY the output reference as it might cause some undefined
                       behavior
        """
