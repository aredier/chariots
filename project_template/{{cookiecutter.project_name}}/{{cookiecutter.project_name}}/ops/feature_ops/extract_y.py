import pandas as pd

from chariots._core import ops, versioning


class ExtractY(ops.AbstractOp):
    """
    op that extracts the target of the iris dataframe
    """

    target_col = versioning.VersionedField([
        "target"
    ], affected_version=versioning.VersionType.MAJOR)

    def execute(self, full_dataset: pd.DataFrame) -> pd.DataFrame:
        return full_dataset.loc[:, self.target_col]
