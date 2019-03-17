from typing import Any
from typing import Optional

from keras import models

from chariots.core import requirements
from chariots.core import saving
from chariots.training import trainable_op

class KerasInput(requirements.Matrix):

    @classmethod
    def combine(cls, left: Any, right: Any) -> Any:
        return [left, right]


class KerasOutput(requirements.Matrix):

    @classmethod
    def combine(cls, left: Any, right: Any) -> Any:
        return [left, right]


class KerasOp(saving.Savable, trainable_op.TrainableOp):
    _model: models.Model = None

    def __init__(self):
        self._model = self._model or self._build_model

    def _main(self, model_input: KerasInput):
        return self._model(model_input)
    
    def _inner_train(self, model_input: KerasInput, model_output: KerasOutput) -> KerasOutput:
        self._model.train_on_batch(model_input, model_output)
    
    def _build_model(self):
        pass

    @classmethod
    def checksum(cls):
        return cls._build_version()
    
    @classmethod
    def identifiers(cls):
        return {"name": cls.name, "model_type": "sklearn"}
