"""loadable op class"""
from . import BaseOp


class LoadableOp(BaseOp):
    """
    an operation that supports loading and saving. This means that when a pipeline tries to load a node using this kind
    of op, it will try to find the serialized bytes of the last saved version of this op and pass them to the  `load`
    method of the op.

    Similarly when the pipeline will try to save a node using this kind of operation, it will get the op's serialized
    bytes by calling it's `serialize` method (along with the op's version)

    to create your own loadable op, you will need to:
    - define the `load` and `serialize` method
    - define the `execute` method as for a normal op to define the behavior of your op
    """

    def execute(self, *args, **kwargs):
        raise NotImplementedError('you must define a call for the op to be valid')

    def load(self, serialized_object: bytes):
        """
        Receives serialize bytes of a newer version of this class and sets the internals of he op accordingly.

        :param serialized_object: the serialized bytes of this op (as where outputed by the `serialize` method
        """
        raise NotImplementedError()

    def serialize(self) -> bytes:
        """
        serializes the object into bytes (to be persisted with a Saver) to be reloaded in the future (you must ensure
        the compatibility with the `load` method

        :return: the serialized bytes representing this operation
        """
        raise NotImplementedError()
