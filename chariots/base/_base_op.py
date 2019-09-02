from typing import List, Any, Optional

from chariots.callbacks import OpCallBack
from chariots.versioning import Version, VersionableMeta


class BaseOp(metaclass=VersionableMeta):
    """
    The ops are the atomic computation units of the Chariots framework. Whereas a `Node` represents a slot in a
    pipeline and the interactions between that spot and the rest of the pipeline, the op will actually be doing the
    computation.

    To subclass the `BaseOp` class and create a new Op, you need to override the `execute` method:

    .. testsetup::

        >>> from chariots.base import BaseOp

    .. doctest::

        >>> class AddOp(BaseOp):
        ...     number_to_add = 1
        ...
        ...     def execute(self, op_input):
        ...         return op_input + self.number_to_add

    and then you can execute the op alone:

    .. doctest::

        >>> AddOp().execute(3)
        4

    or within a pipeline (that can be deployed)

    .. testsetup::

        >>> from chariots import Pipeline
        >>> from chariots.nodes import Node
        >>> from chariots.runners import SequentialRunner
        >>> runner = SequentialRunner()

    .. doctest::

        >>> pipeline = Pipeline([Node(AddOp(), ["__pipeline_input__"], "__pipeline_output__")], "simple_pipeline")
        >>> runner.run(pipeline, 3)  # of course you can use a `Chariots` server to serve our pipeline and op(s)
        4

    The `BaseOp` class is a versioned class (see  the :doc:`versioning <./chariots.versioning>` module for more info)
    so you can use `VersionedField` with it

    .. testsetup::

        >>> from chariots.versioning import VersionType, VersionedField

    .. doctest::

        >>> class AddOp(BaseOp):
        ...     number_to_add = VersionedField(3, VersionType.MAJOR)
        ...
        ...     def execute(self, op_input):
        ...         return op_input + self.number_to_add


        >>> AddOp.__version__
        <Version, major:36d3c, minor: 94e72, patch: 94e72>
        >>> AddOp.number_to_add
        3

    and changing the field will change the version:

    .. doctest::

        >>> class AddOp(BaseOp):
        ...     number_to_add = VersionedField(4, VersionType.MAJOR)
        ...
        ...     def execute(self, op_input):
        ...         return op_input + self.number_to_add


        >>> AddOp.__version__
        <Version, major:8ad66, minor: 94e72, patch: 94e72>


    :param op_callbacks: :doc:`OpCallbacks objects<./chariots.callbacks>` to change the behavior of the op by
                         executing some action before or after the op'execution
    """

    def __init__(self, op_callbacks: Optional[List[OpCallBack]] = None):
       self.callbacks = op_callbacks or []

    def before_execution(self, args: List[Any]):
        """
        method used to create a one-off (compared to using a :doc:`callback<chariots.callbacks>`) custom behavior that
        gets executed before the the op itself

        :param args: the arguments that are going to be passed to the operation
        """
        pass

    def execute(self, *args, **kwargs):
        """
        main method to override.
        it defines the behavior of the op. In the pipeline the argument of the pipeline will be passed from the node
        with one argument per input (in the order of the input nodes)
        """
        raise NotImplementedError("you must define a call for the op to be valid")

    def after_execution(self, args: List[Any], output: Any) -> Any:
        """
        method used to create a one-off (compared to using a :doc:`callback<chariots.callbacks>`) custom behavior that
        gets executed after the the op itself

        :param args: the arguments that were passed to the op
        :param output: the output of the op
        """
        pass

    def execute_with_all_callbacks(self, args):
        """
        executes the op itself alongside all it's callbacks (op callbacks and `before/after_execution` methods)

        :param args: the arguments to be passed to the `execute` method of the op

        :return: the result of the op
        """
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
        """
        return False

    @property
    def name(self) -> str:
        """
        the name of the op.
        this is mainly use to find previous versions and saved ops of this op in the op_store
        """
        return self.__class__.__name__.lower()

    @property
    def op_version(self) -> Version:
        """
        the version the op uses to pass to the pipeline to identify itself. This differs from the `__version__` method
        in that it can add some information besides the class Fields (for instance last training time for ML Ops)
        """
        return self.__version__

    def __str__(self):
        return "<OP {}>".format(self.name)
