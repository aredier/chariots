from typing import Any

from ._base_sk_op import BaseSKOp


class SKUnsupervisedOp(BaseSKOp):
    """
    base class to create unsupervised models using the scikit-learn framework.
    Whatever the mode you will need to link this op with a single upstream node:

    .. testsetup::

        >>> from chariots import Pipeline, MLMode
        >>> from chariots.nodes import Node
        >>> from chariots._helpers.doc_utils import PCAOp, LogisticOp, IrisFullDataSet

    .. doctest::

        >>> train_logistics = Pipeline([
        ...     Node(IrisFullDataSet(), output_nodes=["x", "y"]),
        ...     Node(PCAOp(MLMode.PREDICT), input_nodes=["x"], output_nodes="x_transformed"),
        ...     Node(LogisticOp(MLMode.FIT), input_nodes=["x_transformed", "y"])
        ... ], 'train_logistics')

        >>> pred = Pipeline([
        ...     Node(IrisFullDataSet(),input_nodes=['__pipeline_input__'], output_nodes=["x"]),
        ...     Node(PCAOp(MLMode.PREDICT), input_nodes=["x"], output_nodes="x_transformed"),
        ...     Node(LogisticOp(MLMode.PREDICT), input_nodes=["x_transformed"], output_nodes=['__pipeline_output__'])
        ... ], 'pred')
    """

    def fit(self, X):
        """
        method used to fit the underlying unsupervised model.

        DO NOT TRY TO OVERRIDE THIS METHOD.

        :param X: the dataset (compatible type with the sklearn lib as pandas data-frames or numpy arrays).
        """
        self._model.fit(X)

    def predict(self, X) -> Any:
        """
        transforms the dataset using the underlying unsupervised model

        DO NOT TRY TO OVERRIDE THIS METHOD.

        :param X: the dataset to transform (type must be compatible with the sklearn library such as pandas data frames
                  or numpy arrays).
        """
        return self._model.transform(X)
