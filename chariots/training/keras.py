import os
from typing import Any
from typing import Optional
from typing import IO
from typing import Callable
from typing import Text
from typing import Type
from typing import Optional
from tempfile import TemporaryDirectory

from keras import models

from chariots.core import requirements
from chariots.core import saving
from chariots.training import trainable_op

class KerasInput(requirements.Matrix):
    """
    base Requirement for KerasOp Inputs
    """


    @classmethod
    def combine(cls, left: Any, right: Any) -> Any:
        return [left, right]


class KerasOutput(requirements.Matrix):
    """
    base Requirement for KerasOp outputs
    """

    @classmethod
    def combine(cls, left: Any, right: Any) -> Any:
        return [left, right]


class KerasOp(saving.Savable, trainable_op.TrainableOp):
    _model: models.Model = None

    def __init__(self):
        self._model = self._model or self._build_model()

    def _main(self, model_input: KerasInput) -> KerasOutput:
        return self._model.predict(model_input)
    
    def _inner_train(self, model_input: KerasInput, model_output: KerasOutput) -> KerasOutput:
        self._model.train_on_batch(model_input, model_output)
    
    def _build_model(self):
        pass

    @classmethod
    def checksum(cls):
        saving_version, _ = cls._build_version()
        return saving_version
    
    @classmethod
    def identifiers(cls):
        return {"name": cls.name, "model_type": "sklearn"}

    def _serialize(self, temp_dir: Text):
        self._model.save(os.path.join(temp_dir, "model.h5"))


    @classmethod
    def _deserialize(cls, temp_dir: Text) -> "Savable":
        res = cls()
        res._model = models.load_model(os.path.join(temp_dir, "model.h5"))
        res._is_fited = True
        return res
    
    @classmethod
    def factory(cls, build_function: Callable[[], models.Model], name: Text, doc: Text = "", 
                inputs_requirements: Optional[Type[KerasInput]] = None, 
                output_marker: Optional[Type[KerasOutput]] = None) -> Type["KerasOp"]:
        """creates a KerasOp
        Arguments:
            build_function -- function to ccreate the underlying keras model
            name -- the name of the op
        
        Keyword Arguments:
            doc -- documentation to add to the Op
            input_requirements -- the markers of the input of the underlying model, if None, these 
                will be infered from `_inner_train` signature
            output_markers -- the markers of the output of the underlying model, if None, these 
                will be infered from signature
        
        Returns:
            the resulting op
        """

        res_cls = type(name, (cls,), {"__doc__": doc})
        if inputs_requirements is not None and output_marker is not None:
            res_cls.training_requirements = {"model_input": inputs_requirements, 
                                             "model_output": output_marker}
            res_cls.requires = {"model_input": inputs_requirements}
            res_cls.markers = [output_marker]
        def _build_model(self):
            return build_function()
        res_cls._build_model = _build_model
        return res_cls

