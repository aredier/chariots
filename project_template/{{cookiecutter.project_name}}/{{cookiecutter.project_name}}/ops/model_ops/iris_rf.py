import chariots.versioning
import chariots.versioning._version_type
import chariots.versioning._versioned_field_dict
from chariots.base import versioning
from chariots.sklearn._sk_supervised_op import SKSupervisedOp
from sklearn.ensemble import RandomForestClassifier


class IrisRF(SKSupervisedOp):
    """
    simple random forest model to be used to predict the type of iris
    """
    model_class = RandomForestClassifier
    model_parameters = chariots.versioning._versioned_field_dict.VersionedFieldDict(
        chariots.versioning._version_type.VersionType.MINOR,
        {
            "n_estimators": 5,
            "max_depth": 2
        }
    )
