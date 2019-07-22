from abc import abstractmethod
from enum import Enum
from typing import Any

from chariots.core.ops import LoadableOp
from chariots.core.saving import DillSerializer


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

    serializer_cls = DillSerializer

    def __init__(self, mode: MLMode):
        """
        :param mode: the mode to use when instantiating the op
        """
        self._call_mode = mode
        self.serializer = self.serializer_cls()
        self._model = self._init_model()

    @property
    def mode(self) -> MLMode:
        """
        the mode this op was instantiated with
        """
        return self._call_mode

    def __call__(self, *args, **kwargs):
        if self.mode == MLMode.FIT:
            self.fit(*args, **kwargs)
            return
        if self.mode == MLMode.PREDICT:
            return self.predict(*args, **kwargs)
        if self.mode == MLMode.FIT_PREDICT:
            self.fit(*args, **kwargs)
            return self.predict(*args, **kwargs)
        raise ValueError("unknown mode for {}: {}".format(type(self), self.mode))

    @abstractmethod
    def fit(self, *args, **kwargs):
        """
        fits the op on data (in args and kwargs)
        this method must not return any data (use the FIT_PREDICT mode to predict on the same data the op was trained
        on)
        """
        pass

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

    def load(self, serialized_object: bytes):
        """
        loads the internals of the op with bytes that where saved

        :param serialized_object: the serialized bytes
        """

        self._model = self.serializer.deserialize_object(serialized_object)
        print("loaded")
        print(self._model.predict([[1]]))

    def serialize(self) -> bytes:
        """
        serializes the object into bytes (to be persisted with a Saver) to be reloaded in the future

        :return: the serialized bytes
        """

        print(self._model.predict([[1]]))
        return self.serializer.serialize_object(self._model)
