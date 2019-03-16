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
    _model: models.Model
    imputs = None

    def _main(self, model_input: KerasInput):
        return self._model(model_input)
    
    def _inner_train(self, model_input: KerasInput, model_output: KerasOutput):
        self._model.train_on_batch(model_input, model_output)
