from typing import Any

from chariots.core import versioning
from chariots.ml import ml_op


class BaseSKOp(ml_op.MLOp):

    model_class = None
    model_parameters = versioning.VersionedFieldDict(versioning.VersionType.MAJOR, {})

    def _init_model(self):
        return self.model_class(**self.model_parameters)

    def fit(self, *args, **kwargs):
        raise NotImplementedError("you need to define the fit behavior when subclassing `BaseSKOp`")

    def predict(self, *args, **kwargs) -> Any:
        raise NotImplementedError("you need to define the predict behavior when subclassing `BaseSKOp`")


class SKSupervisedModel(BaseSKOp):

    def fit(self, X, y):
        self._model.fit(X, y)

    def predict(self, X) -> Any:
        return self._model.predict(X).tolist()


class SKUnsupervisedModel(BaseSKOp):

    def fit(self, X):
        self._model.fit(X)

    def predict(self, X) -> Any:
        return self._model.transform(X)
