"""module to support sci-kit learn usupervised models"""
from typing import Any

from ._base_sk_op import BaseSKOp
from .. import versioning


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

    fit_extra_parameters = versioning.VersionedFieldDict(versioning.VersionType.MAJOR, {})

    def fit(self, X):  # pylint: disable=arguments-differ
        """
        method used to fit the underlying unsupervised model.

        DO NOT TRY TO OVERRIDE THIS METHOD.

        :param X: the dataset (compatible type with the sklearn lib as pandas data-frames or numpy arrays).
        """
        self._model.fit(X, **self.fit_extra_parameters)

    def predict(self, X) -> Any:  # pylint: disable=arguments-differ
        """
        transforms the dataset using the underlying unsupervised model

        DO NOT TRY TO OVERRIDE THIS METHOD.

        :param X: the dataset to transform (type must be compatible with the sklearn library such as pandas data frames
                  or numpy arrays).
        """
        return self._model.transform(X)
