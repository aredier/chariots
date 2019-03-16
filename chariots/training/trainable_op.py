from abc import ABCMeta
from abc import abstractmethod
import time

from typing import Optional

from chariots.core.ops import BaseOp
from chariots.core.ops import AbstractOp
from chariots.core.versioning import VersionField
from chariots.core.versioning import VersionType
from chariots.training import TrainableTrait


class TrainableOp(TrainableTrait, BaseOp):
    # TODO find a way to use ABC meta
    """
    abstract base  for all the trainable ops:
    in order to implement you will have to override both the `_main` and `_inner_train`
    which define respectively inference and training behaviours
    the training requirements should represent the data needed in order to train the function
    and their names should correspond to `_inner_train`'s arguments
    """
    
    training_requirements = {}

    # when a breaking change is made in the previous op(s) data structure, if the markers have
    # not changed a trainable op should be able to cope with it
    _carry_on_verision = False

    # which vesion to update when retraining the model 
    # by default this is minor as in most cases all things being equal a retrain doesn't change much
    _last_trained_time = VersionField(VersionType.PATCH, default_factory=lambda:None)

    _is_fited = False

    @property
    def fited(self):
        return self._is_fited
    
    @property
    def ready(self):
        return self.previous_op.ready and self.fited

    @abstractmethod
    def _inner_train(self, **kwargs):
        """
        method that defines the training behavior of the op
        """

    def perform(self) -> "DataSet":
        if not self.fited:
            raise ValueError(f"{self.name} is not fited, cannot perform")
        return super().perform()
    

    def fit(self, other: Optional[AbstractOp] = None):
        """
        method called to train the op on other (which must meet the training requirements)
        """

        # TODO add possibility to fit on unmerged
        reconnect = other is not None
        if reconnect:
            self.previous_op = other

        if self.previous_op is None:
            raise ValueError(f"other is None and {self.name} is not connected to a pipeline")
        if not isinstance(self.previous_op, AbstractOp):
            raise ValueError("call does only work with single ops. if you want another behavior, override the __Call__ method") 
        self._check_compatibility(self.previous_op, self.training_requirements)
        for training_batch in self.previous_op.perform():
           
            args_dict, _ = self._resolve_arguments(training_batch, self.training_requirements)
            self._inner_train(**args_dict)
        self._is_fited = True
        # self._last_trained_time = time.time()
