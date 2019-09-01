from typing import Any

from ._base_sk_op import BaseSKOp


class SKSupervisedOp(BaseSKOp):
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
