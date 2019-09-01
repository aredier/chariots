from typing import List, Any

from chariots.base import BaseOp


class OpCallBack:
    """
    an op callback is used to perform specific nstructions at certain points (before and after) around the operation's
    execution
    """

    def before_execution(self, op: BaseOp, args: List[Any]):
        """
        called before the operation is executed (and before the operation's `before_execution`'s method

        :param op: the operation that is being executed
        :param args: the arguments that are going to be passed to the operation
        """
        pass

    def after_execution(self, op: BaseOp, args: List[Any], output: Any):
        """
        called after the operation has been executed (and after it's `after_execution`'s method.

        :param op: the operation that was executed
        :param args: the arguments that were passed to the op
        :param output: the output the op produced
        """
        pass