from typing import List, Any, Optional

from chariots.core.versioning import VersionableMeta, Version


class AbstractOp(metaclass=VersionableMeta):
    """
    An op represent an atomic unit in a pipeline.
    """

    def __init__(self, callbacks: Optional[List["OpCallBack"]] = None):
        self.callbacks = callbacks or []

    def before_execution(self, args: List[Any]):
        """
        method called before the execution of the main operation (for logging, timings or such). The inputs arguments
        of the operation are provided (do not try to override)

        :param args: the arguments that are going to be passed to the operation
        """
        pass

    def execute(self, *args, **kwargs):
        """
        the method to override to define the behavior of the op (it is what is called in the pipeline)
        """
        raise NotImplementedError("you must define a call for the op to be valid")

    def after_execution(self, args: List[Any], output: Any) -> Any:
        """
        method called just after the execution if the op. The arguments that were passed and the output produced are
        provided (do not try to override)

        :param args: the arguments that were passed to the op
        :param output: the output of the op
        """
        pass

    def execute_with_all_callbacks(self, args):
        self.before_execution(args)
        for callback in self. callbacks:
            callback.before_execution(self, args)
        op_result = self.execute(*args)
        self.after_execution(args, op_result)
        for callback in self. callbacks:
            callback.after_execution(self, args, op_result)
        return op_result

    @property
    def allow_version_change(self):
        """
        whether or not this op accepts to be loaded with the wrong version.
        this is usually False but is useful when loading an op for retraining
        :return:
        """
        return False

    @property
    def name(self) -> str:
        """
        name of the op
        """
        return self.__class__.__name__.lower()

    @property
    def op_version(self) -> Version:
        return self.__version__

    def __str__(self):
        return "<OP {}>".format(self.name)


class LoadableOp(AbstractOp):

    def execute(self, *args, **kwargs):
        raise NotImplementedError("you must define a call for the op to be valid")

    def load(self, serialized_object: bytes):
        """
        loads the internals of the op with bytes that where saved

        :param serialized_object: the serialized bytes
        """
        raise NotImplementedError()

    def serialize(self) -> bytes:
        """
        serializes the object into bytes (to be persisted with a Saver) to be reloaded in the future

        :return: the serialized bytes
        """
        raise NotImplementedError()


class OpCallBack:
    """
    an op callback is used to perform specific nstructions at certain points (before and after) around the operation's
    execution
    """

    def before_execution(self, op: AbstractOp, args: List[Any]):
        """
        called before the operation is executed (and before the operation's `before_execution`'s method

        :param op: the operation that is being executed
        :param args: the arguments that are going to be passed to the operation
        """
        pass

    def after_execution(self, op: AbstractOp, args: List[Any], output: Any):
        """
        called after the operation has been executed (and after it's `after_execution`'s method.

        :param op: the operation that was executed
        :param args: the arguments that were passed to the op
        :param output: the output the op produced
        """
        pass
