"""base class for sci-kit learn ops"""
from typing import Any

from ... import versioning
from .. import BaseMLOp


class BaseSKOp(BaseMLOp):
    """
    base Op class for all the supervised and unsupervised scikit-learn ops
    """

    # the class of the model to fit/predict
    model_class = None  # type: sklearn.base.BaseEstimator
    # the parameters to use to init the model
    model_parameters = versioning.VersionedFieldDict(versioning.VersionType.MAJOR, {})

    def _init_model(self):
        """
        initialises the model
        :return the initialised model
        """
        return self.model_class(**self.model_parameters)  # pylint: disable=not-callable

    def fit(self, *args, **kwargs):
        raise NotImplementedError('you need to define the fit behavior when subclassing `BaseSKOp`')

    def predict(self, *args, **kwargs) -> Any:
        raise NotImplementedError('you need to define the predict behavior when subclassing `BaseSKOp`')
