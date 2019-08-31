from chariots._core import versioning
from chariots._ml.sklearn_op import SKSupervisedModel
from sklearn.ensemble import RandomForestClassifier


class IrisRF(SKSupervisedModel):
    """
    simple random forest model to be used to predict the type of iris
    """
    model_class = RandomForestClassifier
    model_parameters = versioning.VersionedFieldDict(
        versioning.VersionType.MINOR,
        {
            "n_estimators": 5,
            "max_depth": 2
        }
    )
