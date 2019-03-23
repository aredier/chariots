import json
import os
import inspect
import time
from abc import ABCMeta, abstractmethod
from typing import Optional,Text

from chariots.core.ops import AbstractOp, BaseOp
from chariots.core.versioning import SubVersionType, VersionField, VersionType, Version
from chariots.training import TrainableTrait, evaluation
from chariots.core.saving import Savable


class TrainableOp(Savable, TrainableTrait, BaseOp):
    # TODO find a way to use ABC meta
    """
    abstract base  for all the trainable ops:
    in order to implement you will have to override both the `_main` and `_inner_train`
    which define respectively inference and training behaviours
    the training requirements should represent the data needed in order to train the function
    and their names should correspond to `_inner_train`'s arguments
    """

    training_requirements = None

    # when a breaking change is made in the previous op(s) data structure, if the markers have
    # not changed a trainable op should be able to cope with it
    _carry_on_verision = False

    # which vesion to update when retraining the model 
    # by default this is minor as in most cases all things being equal a retrain doesn't change much
    _last_trained_time = VersionField(SubVersionType.PATCH, target_version=VersionType.RUNTIME,
                                      default_factory=lambda:None)

    # in case of coupeling after train (mostly after saving) to know if we accept the previous
    # version or if we need to retrain
    _upstream_version_str_at_train = None

    _is_fited = False
    evaluation_metric = None

    @classmethod
    def _interpret_signature(cls):
        super()._interpret_signature()
        if cls.training_requirements is None:
            training_sig = inspect.signature(cls._inner_train)
            cls.training_requirements = cls._find_valid_requirements(training_sig)


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

    def attach_evaluation(self, evaluation: evaluation.EvaluationMetric):
        self.evaluation_metric = evaluation

    def evaluate(self, data: AbstractOp):
        if self.evaluation_metric is None:
            raise ValueError("cannot evaluate a trainable op with no metric linked to it,"\
                             " use `attach_evaluation`")
        self(data)
        evaluation_res = self.evaluation_metric.evaluate(self)
        self.previous_op = None
        return evaluation_res

    def __call__(self, other):
        if self.fited:
            for version_string in self._upstream_version_str_at_train:
                parsed_version = Version.parse(version_string)
                if all(parsed_version.major != Version.parse(potential_version).major
                       for _, potential_version in other.compounded_markers_and_version_str):
                    raise ValueError("op fitted on a different version, consider retraining")
        return super().__call__(other)

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
        self._last_trained_time = time.time()
        self._upstream_version_str_at_train = self._find_previous_prediction_op_version()

    def _find_previous_prediction_op_version(self):
        self.compounded_markers_and_version_str
        return [version for req, version in self.previous_op.compounded_markers_and_version_str
                if any(requirement.compatible(req) for requirement in self.requires.values())]

    @classmethod
    def checksum(cls):
        saving_version, _ = cls._build_version()
        return saving_version

    def _serialize(self, temp_dir: Text):
        self.saving_version.save_fields(os.path.join(temp_dir, "_runtime_version.json"))
        with open(os.path.join(temp_dir, "_upstream_version_str_at_train.json"), "w") as file:
            json.dump(self._upstream_version_str_at_train, file)

    @classmethod
    def _deserialize(cls, temp_dir: Text) -> "TrainableOp":
        instance = cls()
        with open(os.path.join(temp_dir, "_upstream_version_str_at_train.json"), "r") as file:
            instance._upstream_version_str_at_train = json.load(file)
        versioned_fields = instance.saving_version.load_fields(os.path.join(temp_dir,
                                                               "_runtime_version.json"))
        for field_name, field_value in versioned_fields.items(): 
            setattr(instance, field_name, field_value)
        return instance

