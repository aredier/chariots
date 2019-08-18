from chariots.core import versioning
from chariots.ml.sklearn_op import SKUnsupervisedModel
from sklearn.decomposition import PCA


class IrisPCA(SKUnsupervisedModel):
    """
    simple PCA to be used in train/predict pipelines
    """

    model_class = PCA
    model_parameters = versioning.VersionedFieldDict(versioning.VersionType.MAJOR, {
        "n_components": 2,
    })

