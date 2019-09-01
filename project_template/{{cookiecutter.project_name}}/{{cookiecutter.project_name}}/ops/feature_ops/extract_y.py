import pandas as pd

from chariots.base import BaseOp
from chariots.versioning import VersionedField, VersionType


class ExtractY(BaseOp):
    """
    op that extracts the target of the iris dataframe
    """

    target_col = VersionedField(["target"], affected_version=VersionType.MAJOR)

    def execute(self, full_dataset: pd.DataFrame) -> pd.DataFrame:
        return full_dataset.loc[:, self.target_col]
