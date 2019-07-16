from abc import abstractmethod
from enum import Enum

from chariots.core.ops import LoadableOp
from chariots.core.saving import DillSerializer


class MLMode(Enum):
    FIT = "fit"
    PREDICT = "predict"


class MLOp(LoadableOp):

    serializer_cls = DillSerializer

    def __init__(self, mode: Enum):
        self._call_mode = mode
        self.serializer = self.serializer_cls()
        self._model = self._init_model()

    @property
    def mode(self):
        return self._call_mode

    def __call__(self, *args, **kwargs):
        if self.mode == MLMode.FIT:
            return self.fit(*args, **kwargs)
        if self.mode == MLMode.PREDICT:
            return self.predict(*args, **kwargs)
        raise ValueError("unknown mode for {}: {}".format(type(self), self.mode))

    @abstractmethod
    def fit(self, *args, **kwargs):
        pass

    @abstractmethod
    def predict(self, *args, **kwargs):
        pass

    @abstractmethod
    def _init_model(self):
        pass

    def load(self, serialized_object: bytes):
        self._model = self.serializer.deserialize_object(serialized_object)
        print("loaded")
        print(self._model.predict([[1]]))


    def serialize(self) -> bytes:
        print(self._model.predict([[1]]))
        return self.serializer.serialize_object(self._model)
