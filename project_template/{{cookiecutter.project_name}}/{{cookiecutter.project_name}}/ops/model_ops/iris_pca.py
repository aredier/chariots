from sklearn.decomposition import PCA

from chariots.versioning import VersionType, VersionedFieldDict
from chariots.sklearn._sk_unsupervised_op import SKUnsupervisedOp


class IrisPCA(SKUnsupervisedOp):
    """
    simple PCA to be used in train/predict pipelines
    """

    model_class = PCA
    model_parameters = VersionedFieldDict(
        VersionType.MAJOR,
        {
            "n_components": 2,
        }
    )
