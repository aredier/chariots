"""data saving node"""
from typing import Optional, List, Any

from chariots.base import BaseSerializer, BaseRunner, BaseSaver
from chariots.versioning import Version
from ._data_node import DataNode
from .._helpers.typing import InputNodes


class DataSavingNode(DataNode):
    """
    a node for saving data into the app's Saver (if used in an app, otherwise use the `attach_save` method to define
    this node's saver).

    You can use this node like any other node except that it doesn't take a `input_nodes` parameters

    .. testsetup::

        >>> import tempfile
        >>> import shutil


        >>> from chariots import Pipeline
        >>> from chariots.nodes import Node, DataLoadingNode
        >>> from chariots.runners import SequentialRunner
        >>> from chariots.savers import FileSaver
        >>> from chariots.serializers import CSVSerializer
        >>> from chariots._helpers.doc_utils import AnalyseDataSetOp, TrainTestSplit, DataSavingNode, DillSerializer
        >>> from chariots._helpers.doc_utils import IrisDF

        >>> app_path = tempfile.mkdtemp()

        >>> saver = FileSaver(app_path)
        >>> runner = SequentialRunner()

    .. doctest::

        >>> save_train_test = Pipeline([
        ...     Node(IrisDF(), output_nodes='df'),
        ...     Node(TrainTestSplit(), input_nodes=['df'], output_nodes=['train_df', 'test_df']),
        ...     DataSavingNode(serializer=CSVSerializer(), path='/train.csv', input_nodes=['train_df']),
        ...     DataSavingNode(serializer=DillSerializer(), path='/test.pkl', input_nodes=['test_df'])
        ... ], "save")

    you can then use the prepare method of the pipeline to attach a saver to our various `DataNodes` and run the
    pipeline like any other

    .. doctest::

        >>> save_train_test.prepare(saver)
        >>> runner.run(save_train_test)

    .. testsetup::

        >>> shutil.rmtree(app_path)

    :param saver: the saver to use for loading or saving data (if not specified at init, you can use the
                  `attach_saver` method
    :param serializer: the serializer to use to load the dat
    :param path: the path to load the data from
    :param input_nodes: the data that needs to be saved
    :param name: the name of the op
    """

    def __init__(self, serializer: BaseSerializer, path: str,  # pylint: disable=too-many-arguments
                 input_nodes: Optional[InputNodes],
                 name: Optional[str] = None, saver: Optional[BaseSaver] = None):
        super().__init__(serializer=serializer, path=path, input_nodes=input_nodes, name=name, saver=saver)

    @property
    def node_version(self) -> Version:
        return Version()

    def execute(self, params: List[Any],  # pylint: disable=arguments-differ
                runner: Optional[BaseRunner] = None) -> Any:
        if self._saver is None:
            raise ValueError('cannot save data without a saver')
        self._saver.save(self.serializer.serialize_object(params[0]), self.path)

    def __repr__(self):
        return '<DataLoadingNode of {}>'.format(self.path)
