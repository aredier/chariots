from hashlib import sha1
from typing import Optional, List, Any

from chariots.base import BaseSerializer, BaseRunner, BaseSaver
from chariots.versioning import Version
from ._data_node import DataNode


class DataLoadingNode(DataNode):
    """
    a node for loading data from the ap's saver (if used in an app, otherwise use the `attach_save` method to define
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
        >>> from chariots._helpers.doc_utils import IrisDF, save_train_test

        >>> app_path = tempfile.mkdtemp()

        >>> saver = FileSaver(app_path)
        >>> runner = SequentialRunner()
        >>> save_train_test.prepare(saver)
        >>> runner.run(save_train_test)

    .. doctest::

        >>> load_and_analyse_iris = Pipeline([
        ...     DataLoadingNode(serializer=CSVSerializer(), path='/train.csv', output_nodes=["train_df"]),
        ...     Node(AnalyseDataSetOp(), input_nodes=["train_df"], output_nodes=["__pipeline_output__"]),
        ... ], "analyse")

    then you can prepare the pipeline (which attaches the saver) and run the pipeline

    .. doctest::

        >>> load_and_analyse_iris.prepare(saver)
        >>> runner.run(load_and_analyse_iris)
        Counter({1: 39, 2: 38, 0: 35})

    .. testsetup::

        >>> shutil.rmtree(app_path)

    :param saver: the saver to use for loading or saving data (if not specified at init, you can use the
                  `attach_saver` method
    :param serializer: the serializer to use to load the dat
    :param path: the path to load the data from
    :param output_nodes: an optional symbolic name for the node to be called by other node. If this node is the
                         output of the pipeline use "pipeline_output" or `ReservedNodes.pipeline_output`
    :param name: the name of the op
    """

    def __init__(self, serializer: BaseSerializer, path: str, output_nodes=None, name: Optional[str] = None,
                 saver: Optional[BaseSaver] = None):
        super().__init__(serializer=serializer, path=path, output_nodes=output_nodes, name=name, saver=saver)

    @property
    def node_version(self) -> Version:
        if self._saver is None:
            raise ValueError("cannot get the version of a data op without a saver")
        version = Version()
        file_hash = sha1(self._saver.load(self.path)).hexdigest()
        version.update_major(file_hash.encode("utf-8"))
        return version

    def execute(self, params: List[Any], runner: Optional[BaseRunner] = None) -> Any:

        if self._saver is None:
            raise ValueError("cannot load data without a saver")
        return self.serializer.deserialize_object(self._saver.load(self.path))

    def __repr__(self):
        return "<DataLoadingNode of {}>".format(self.path)
