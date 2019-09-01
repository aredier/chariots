import pandas as pd

import chariots.versioning
import chariots.versioning._version_type
import chariots.versioning._versioned_field
from chariots.base import _base_op, versioning


class ExtractY(_base_op.BaseOp):
    """
    op that extracts the target of the iris dataframe
    """

    target_col = chariots.versioning._versioned_field.VersionedField([
        "target"
    ], affected_version=chariots.versioning._version_type.VersionType.MAJOR)

    def execute(self, full_dataset: pd.DataFrame) -> pd.DataFrame:
        return full_dataset.loc[:, self.target_col]
