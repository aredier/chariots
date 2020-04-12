"""
Module that handles all of the Machine learning integration.

This modeule provides helpers for the most popular ML frameworks (sci-kit learn and keras for now) as well as the
`BaseMlOp` class to allow you to create Ops for non supported frameworks (or custom algorithms)
"""
from ._ml_mode import MLMode
from . import serializers
from ._base_ml_op import BaseMLOp
from . import sklearn
from . import keras

__all__ = [
    'MLMode',
    'BaseMLOp',
    'serializers',
    'sklearn',
    'keras'
]
