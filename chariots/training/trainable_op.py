from abc import ABCMeta
from abc import abstractmethod

from chariots.core.ops import BaseOp
from chariots.core.ops import AbstractOp


class TrainableOp(BaseOp):
    # TODO find a way to use ABC meta
    """
    abstract base  for all the trainable ops:
    in order to implement you will have to override both the `_main` and `_inner_train`
    which define respectively inference and training behaviours
    the training requirements should represent the data needed in order to train the function
    and their names should correspond to `_inner_train`'s arguments
    """
    
    training_requirements = {}

    @abstractmethod
    def _inner_train(self, **kwargs):
        """
        method that defines the training behavior of the op
        """
    

    def fit(self, other: AbstractOp):
        """
        method called to train the op on other (which must meet the training requirements)
        """
        if not isinstance(other, AbstractOp):
            raise ValueError("call does only work with single ops. if you want another behavior, override the __Call__ method") 
        self._check_compatibility(other, self.training_requirements)
        self.previous_op = other
        for training_batch in self.previous_op.perform():
            args_dict = self._resolve_arguments(training_batch, self.training_requirements)
            self._inner_train(**args_dict)
