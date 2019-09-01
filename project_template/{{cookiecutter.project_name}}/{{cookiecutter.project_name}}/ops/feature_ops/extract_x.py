import pandas as pd

from chariots.base import BaseOp
from chariots.versioning import VersionType, VersionedField


class ExtractX(BaseOp):
    """
    op that extracts the relevant X from a pd DataFrame representing the
    iris data set
    """

    train_cols = VersionedField([
        'sepal length (cm)',
        'sepal width (cm)',
        'petal length (cm)',
        'petal width (cm)'
    ], affected_version=VersionType.MAJOR)

    def execute(self, full_dataset: pd.DataFrame) -> pd.DataFrame:
        return full_dataset.loc[:, self.train_cols]
