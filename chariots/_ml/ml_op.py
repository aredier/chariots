import io
import json
import time
from abc import abstractmethod
from enum import Enum
from typing import Any, List, Optional
from zipfile import ZipFile

from chariots._core import versioning
from chariots._core.ops import LoadableOp, OpCallBack
from chariots._core.saving import DillSerializer


class MLMode(Enum):
    """
    mode in which to put the op (prediction of training) enum
    """
    FIT = "fit"
    PREDICT = "predict"
    FIT_PREDICT = "fit_predict"


class MLOp(LoadableOp):
    """
    an MLOp is an op that has three distinctive modes:

    - fitting that trains the inner model
    - prediction where the op is used to make prediction
    - fit predict which passes the arguments to the predict method after the fiting

    the usual worklow is to a have a training and a prediction pipeline. and to:

    - execute the training pipeline:
    - save the training pipeline
    - reload the prediction pipeline
    - use the prediction pipeline
    """

    training_update_version = versioning.VersionType.PATCH
    serializer_cls = DillSerializer

    def __init__(self, mode: MLMode, callbacks: Optional[List[OpCallBack]] = None):
        """
        :param mode: the mode to use when instantiating the op
        """
        super().__init__(callbacks)
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
        """
        the mode this op was instantiated with
        """
        return self._call_mode

    def execute(self, *args, **kwargs):
        if self.mode == MLMode.FIT:
            self._fit(*args, **kwargs)
            return
        if self.mode == MLMode.PREDICT:
            return self.predict(*args, **kwargs)
        if self.mode == MLMode.FIT_PREDICT:
            self._fit(*args, **kwargs)
            return self.predict(*args, **kwargs)
        raise ValueError("unknown mode for {}: {}".format(type(self), self.mode))

    def _fit(self, *args, **kwargs):
        """
        method that wraps fit and performs necessary actions (update version)
        """
        self.fit(*args, **kwargs)
        self._last_training_time = time.time()

    @abstractmethod
    def fit(self, *args, **kwargs):
        """
        fits the op on data (in args and kwargs)
        this method must not return any data (use the FIT_PREDICT mode to predict on the same data the op was trained
        on)
        """

    @abstractmethod
    def predict(self, *args, **kwargs) -> Any:
        """
        the mehtod used to do predictions/inference once the model has been fitted/loaded
        :return: the inference
        """
        pass

    @abstractmethod
    def _init_model(self):
        """
        method used to create the model (used at initialisation)
        """
        pass

    @property
    def op_version(self):
        time_version = versioning.Version().update(self.training_update_version,
                                                   str(self._last_training_time).encode("utf-8"))
        return super().op_version + time_version

    def load(self, serialized_object: bytes):
        """
        loads the internals of the op with bytes that where saved

        :param serialized_object: the serialized bytes
        """

        io_file = io.BytesIO(serialized_object)
        with ZipFile(io_file, "r") as zip_file:
            self._model = self.serializer   .deserialize_object(zip_file.read("model"))
            meta = json.loads(zip_file.read("_meta.json").decode("utf-8"))
            self._last_training_time = meta["train_time"]

    def serialize(self) -> bytes:
        """
        serializes the object into bytes (to be persisted with a Saver) to be reloaded in the future

        :return: the serialized bytes
        """
        io_file = io.BytesIO()
        with ZipFile(io_file, "w") as zip_file:
            zip_file.writestr("model", self.serializer.serialize_object(self._model))
            zip_file.writestr("_meta.json", json.dumps({"train_time": self._last_training_time}))
        return io_file.getvalue()
