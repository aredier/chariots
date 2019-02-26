from abc import ABCMeta
from abc import abstractmethod

from chariots.core.ops import BaseOp
from chariots.core.ops import AbstractOp


class TrainableOp(BaseOp):
    training_requirements = {}

    @abstractmethod
    def _train_function(self, **kwargs):
        pass
    

    def fit(self, other: AbstractOp):
        if not isinstance(other, AbstractOp):
            raise ValueError("call does only work with single ops. if you want another behavior, override the __Call__ method") 
        self._check_compatibility(other, self.training_requirements)
        self.previous_op = other
        for training_batch in self.previous_op.perform():
            args_dict = self._resolve_arguments(training_batch, self.training_requirements)
            self._train_function(**args_dict)