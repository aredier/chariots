from chariots.base import BaseOp


class LoadableOp(BaseOp):

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
