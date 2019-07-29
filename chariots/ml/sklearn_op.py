from typing import Any

from chariots.core import versioning
from chariots.ml import ml_op


class SKSupervisedModel(ml_op.MLOp):

    model_class = None
    model_parameters = versioning.VersionedFieldDict(versioning.VersionType.MAJOR, {})

    def fit(self, X, y):
        self._model.fit(X, y)

    def predict(self, X) -> Any:
        self._model.predict(X)

    def _init_model(self):
        return self.model_class(**self.model_parameters)
