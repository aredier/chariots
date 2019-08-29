import pandas as pd
from chariots.core import ops, versioning


class ExtractX(ops.AbstractOp):
    """
    op that extracts the relevant X from a pd DataFrame representing the
    iris data set
    """

    train_cols = versioning.VersionedField([
        'sepal length (cm)',
        'sepal width (cm)',
        'petal length (cm)',
        'petal width (cm)'
    ], affected_version=versioning.VersionType.MAJOR)

    def execute(self, full_dataset: pd.DataFrame) -> pd.DataFrame:
        return full_dataset.loc[:, self.train_cols]
