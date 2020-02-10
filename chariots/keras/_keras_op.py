"""Keras Op class"""
from typing import Any, List, Union, Optional

import numpy as np

from chariots import MLMode
from chariots.base import BaseMLOp
from chariots.versioning import VersionedFieldDict


class KerasOp(BaseMLOp):
    """
    Keras Ops help you create ops for all your Keras based neural networks.

    To create your keras op, you will need to:

    - define the initialisation behavior of your model by overriding the `_init_model` method.
    - define any additional training parameters using the `fit_params` `VersionedFieldDict`.

    .. testsetup::

        >>> from chariots._helpers.doc_utils import IrisFullDataSet, Categorize

    .. doctest::

        >>> from chariots import Pipeline, MLMode
        >>> from chariots.keras import KerasOp
        >>> from chariots.nodes import Node
        >>> from chariots.versioning import VersionType, VersionedFieldDict
        >>> from keras import models, layers
        ...
        ...
        >>> class KerasLinear(KerasOp):
        ...     fit_params = VersionedFieldDict(VersionType.MAJOR, {
        ...         'epochs': 3,
        ...         'batch_size': 32,
        ...     })
        ...
        ...     def _init_model(self, *input_data_sets):
        ...         model = models.Sequential([layers.Dense(3, activation='softmax', input_shape=(4,))])
        ...         model.compile(loss='categorical_crossentropy', optimizer='adam')
        ...         return model
        ...
        ...
        >>> train = Pipeline([
        ...     Node(IrisFullDataSet(), output_nodes=["X", "y"]),
        ...     Node(Categorize(), input_nodes=['y'], output_nodes='y_cat'),
        ...     Node(KerasLinear(mode=MLMode.FIT, verbose=0), input_nodes=['X', 'y_cat'])
        ... ], 'train')
        >>> pred = Pipeline([
        ...     Node(KerasLinear(mode=MLMode.PREDICT), input_nodes=['__pipeline_input__'],
        ...          output_nodes='__pipeline_output__')
        ... ], 'pred')

    than you can call your pipeline as you would with any other:

    .. testsetup::

        >>> import tempfile
        >>> import shutil
        ...
        >>> import numpy as np
        >>> from chariots.runners import SequentialRunner
        >>> from chariots import Chariots
        ...
        ...
        >>> app_path = tempfile.mkdtemp()
        >>> runner = SequentialRunner()

    .. doctest::

        >>> runner.run(train)
        ...
        >>> runner.run(pred, np.array([[1, 2, 3, 4]])) # doctest: +ELLIPSIS
        array([[...]], dtype=float32)

    or use them in an app:

    .. doctest::

        >>> app = Chariots([train, pred], app_path, import_name='my_app')

    .. testsetup::

        >>> shutil.rmtree(app_path)

    """

    input_params = VersionedFieldDict()

    def __init__(self, mode: MLMode, verbose: Optional[int] = 1):
        super().__init__(mode)
        self.verbose_level = verbose

    def fit(self, input_data_sets: Union[List[np.ndarray], np.ndarray],  # pylint: disable=arguments-differ
            output_datasets: Union[List[np.ndarray], np.ndarray]):
        self._model.fit(input_data_sets, output_datasets, verbose=self.verbose_level, **self.input_params)

    def predict(self, input_datasets) -> Any:  # pylint: disable=arguments-differ
        return self._model.predict(input_datasets)

    def _init_model(self):
        raise NotImplementedError('you need to define the initialisation behavior of your NN')
