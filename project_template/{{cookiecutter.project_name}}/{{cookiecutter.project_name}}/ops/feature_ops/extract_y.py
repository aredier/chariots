import pandas as pd
from chariots.core import ops, versioning


class ExtractY(ops.AbstractOp):
    """
    op that extracts the target of the iris dataframe
    """

    target_col = versioning.VersionedField([
        "target"
    ], affected_version=versioning.VersionType.MAJOR)

    def __call__(self, full_dataset: pd.DataFrame) -> pd.DataFrame:
        return full_dataset.loc[:, self.target_col]
