from typing import Any

from ._base_sk_op import BaseSKOp


class SKUnsupervisedOp(BaseSKOp):

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