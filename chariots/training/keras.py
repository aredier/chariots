import os
from typing import Any
from typing import Optional
from typing import IO
from tempfile import TemporaryDirectory

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

    def _serialize(self, temp_file: IO):
        with TemporaryDirectory() as temp_directory:
            transition_file = os.path.join(temp_directory, "model.h5")
            self._model.save(transition_file)
            with open(transition_file, "r") as transition_file:
                temp_file.write(transition_file.read())


    @classmethod
    def _deserialize(cls, file: IO) -> "Savable":
        with TemporaryDirectory() as temp_directory:
            transition_file = os.path.join(temp_directory, "model.h5")
            with open(transition_file, "r") as transition_file:
                transition_file.write(file.read)
            model = models.load_model(transition_file)
        res = cls()
        res._model = model
        return res
    
    @classmethod
    def factory(cls, build_function, name, doc="", inputs_marker = None, output_marker = None):
        return type(name, (cls,), {
            "training_requirements": {
                "model_input": inputs_marker,
                "model_output": output_marker
                },
            "markers": output_marker,
            "__doc__": doc,
        })

