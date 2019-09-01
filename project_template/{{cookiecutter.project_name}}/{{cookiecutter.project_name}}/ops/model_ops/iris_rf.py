from chariots.versioning import VersionType, VersionedFieldDict
from chariots.sklearn._sk_supervised_op import SKSupervisedOp
from sklearn.ensemble import RandomForestClassifier


class IrisRF(SKSupervisedOp):
    """
    simple random forest model to be used to predict the type of iris
    """
    model_class = RandomForestClassifier
    model_parameters = VersionedFieldDict(VersionType.MINOR, {"n_estimators": 5, "max_depth": 2})
