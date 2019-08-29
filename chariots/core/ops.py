from chariots.core.versioning import VersionableMeta, Version


class AbstractOp(metaclass=VersionableMeta):
    """
    An op represent an atomic unit in a pipeline.
    """

    def execute(self, *args, **kwargs):
        """
        the method to override to define the behavior of the op (it is what is called in the pipeline)
        """
        raise NotImplementedError("you must define a call for the op to be valid")

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
    pass