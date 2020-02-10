"""machine learning abstract Ops"""
import io
import json
import time
from abc import abstractmethod
from typing import Optional, List, Any
from zipfile import ZipFile

from chariots.callbacks import OpCallBack
from chariots.serializers import DillSerializer
from chariots.versioning import Version, VersionType
from .._ml_mode import MLMode
from ..ops._loadable_op import LoadableOp


class BaseMLOp(LoadableOp):
    """
    an BaseMLOp are ops designed specifically to be machine learning models (whether for training or inference). You
    can initialize the op in three distinctive :doc:`ml mode <./chariots>`:

    - `FIT` for training the model
    - `PREDICT` to perform inference
    - `FIT_PREDICT` to do both (train and predict on the same dataset

    the usual workflow is to a have a training and a prediction pipeline. and to:

    - execute the training pipeline:
    - save the training pipeline
    - reload the prediction pipeline
    - use the prediction pipeline

    here is an example:

    .. testsetup::

        >>> from chariots.nodes import Node
        >>> from chariots import Pipeline
        >>> from chariots._helpers.doc_utils import IrisFullDataSet, PCAOp, LogisticOp

    first create your pipelines:

    .. doctest::

        >>> train = Pipeline([
        ...     Node(IrisFullDataSet(), output_nodes=["x", "y"]),
        ...     Node(PCAOp(MLMode.FIT_PREDICT), input_nodes=["x"], output_nodes="x_transformed"),
        ...     Node(LogisticOp(MLMode.FIT), input_nodes=["x_transformed", "y"])
        ... ], 'train')

        >>> pred = Pipeline([
        ...     Node(PCAOp(MLMode.PREDICT), input_nodes=["__pipeline_input__"], output_nodes="x_transformed"),
        ...     Node(LogisticOp(MLMode.PREDICT), input_nodes=["x_transformed"], output_nodes=['__pipeline_output__'])
        ... ], 'pred')

    .. testsetup::

        >>> import tempfile
        >>> import shutil

        >>> from chariots import Chariots, TestClient

        >>> app_path = tempfile.mkdtemp()
        >>> app = Chariots([train, pred], app_path, import_name="app")
        >>> client = TestClient(app)

    and then to train your pipelines and make some predictions:

    .. doctest::

        >>> response = client.call_pipeline(train)
        >>> client.save_pipeline(train)
        >>> client.load_pipeline(pred)
        >>> response = client.call_pipeline(pred, [[1, 2, 3, 4]])
        >>> response.value
        [1]

    If you want to create a new MLOp class (to accommodate an unsupported framework for instance), you need to define:

    - how to fit your op with the `fit` method
    - how to perform inference with your op with the `predict` method
    - define how to initialize a new model with the `_init_model` method

    and eventually you can change the `serializer_cls` class attribute to change the serialization format of your model

    :param op_callbacks: :doc:`OpCallbacks objects<./chariots.callbacks>` to change the behavior of the op by
                         executing some action before or after the op's execution
    """

    training_update_version = VersionType.PATCH
    serializer_cls = DillSerializer

    def __init__(self, mode: MLMode, op_callbacks: Optional[List[OpCallBack]] = None):
        """
        :param mode: the mode to use when instantiating the op
        """
        super().__init__(op_callbacks)
        self._call_mode = mode
        self.serializer = self.serializer_cls()
        self._model = self._init_model()
        self._last_training_time = 0

    @property
    def allow_version_change(self):
        # we only want to check the version on prediction
        return self.mode != MLMode.PREDICT

    @property
    def mode(self) -> MLMode:
        """the mode this op was instantiated with"""
        return self._call_mode

    def execute(self, *args, **kwargs):
        """
        executes the model action that is required (train, test or both depending in what the op was initialized with
        """
        if self.mode == MLMode.FIT:
            self._fit(*args, **kwargs)
            return None
        if self.mode == MLMode.PREDICT:
            return self.predict(*args, **kwargs)
        if self.mode == MLMode.FIT_PREDICT:
            self._fit(*args, **kwargs)
            return self.predict(*args, **kwargs)
        raise ValueError('unknown mode for {}: {}'.format(type(self), self.mode))

    def _fit(self, *args, **kwargs):
        """
        method that wraps fit and performs necessary actions (update version)
        """
        self.fit(*args, **kwargs)
        self._last_training_time = time.time()

    @abstractmethod
    def fit(self, *args, **kwargs):
        """
        fits the inner model of the op on data (in args and kwargs)
        this method must not return any data (use the FIT_PREDICT mode to predict on the same data the op was trained
        on)
        """

    @abstractmethod
    def predict(self, *args, **kwargs) -> Any:
        """
        the method used to do predictions/inference once the model has been fitted/loaded
        """

    @abstractmethod
    def _init_model(self):
        """
        method used to create a new (blank) model (used at initialisation)
        """

    @property
    def op_version(self):
        time_version = Version().update(self.training_update_version,
                                        str(self._last_training_time).encode('utf-8'))
        return super().op_version + time_version

    def load(self, serialized_object: bytes):

        io_file = io.BytesIO(serialized_object)
        with ZipFile(io_file, 'r') as zip_file:
            self._model = self.serializer   .deserialize_object(zip_file.read('model'))
            meta = json.loads(zip_file.read('_meta.json').decode('utf-8'))
            self._last_training_time = meta['train_time']

    def serialize(self) -> bytes:
        io_file = io.BytesIO()
        with ZipFile(io_file, 'w') as zip_file:
            zip_file.writestr('model', self.serializer.serialize_object(self._model))
            zip_file.writestr('_meta.json', json.dumps({'train_time': self._last_training_time}))
        return io_file.getvalue()
