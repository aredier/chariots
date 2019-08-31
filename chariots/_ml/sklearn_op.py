from typing import Any

from chariots._core import versioning
from chariots._ml import ml_op


class BaseSKOp(ml_op.MLOp):
    """
    base Op class for all the supervised and unsupervised scikit-learn ops
    """

    # the class of the model to fit/predict
    model_class = None
    # the parameters to use to init the model
    model_parameters = versioning.VersionedFieldDict(versioning.VersionType.MAJOR, {})

    def _init_model(self):
        """
        initialises the model
        :return the initialised model
        """
        return self.model_class(**self.model_parameters)

    def fit(self, *args, **kwargs):
        raise NotImplementedError("you need to define the fit behavior when subclassing `BaseSKOp`")

    def predict(self, *args, **kwargs) -> Any:
        raise NotImplementedError("you need to define the predict behavior when subclassing `BaseSKOp`")


class SKSupervisedModel(BaseSKOp):
    """
    Op that represent a scikit-learn supervised model
    """

    def fit(self, X, y):
        """
        method used when the op is in train mode
        """
        self._model.fit(X, y)

    def predict(self, X) -> Any:
        """
        method used when the op is in predict mode (to perform inference)
        """
        return self._model.predict(X).tolist()


class SKUnsupervisedModel(BaseSKOp):

    def fit(self, X):
        """
        method used when the op is in train mode
        """
        self._model.fit(X)

    def predict(self, X) -> Any:
        """
        method used when the op is in predict mode (to transform X)
        """
        return self._model.transform(X)
