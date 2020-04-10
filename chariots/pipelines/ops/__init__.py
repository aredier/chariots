"""
operations are the atomic computation element of Chariots, you can use them to train models, preprocess your data,
extract features and much more.

to create your own operations, you will need to subclass one of the base op classes:

- create a minimalist operation by subclassing the :doc:`BaseOp class<./chariots.base>`
- create an op that supports loading and saving by subclassing the `LoadableOp` class
- create a machine learning operation by subclassing on of the machine learning ops (depending on your framework) like
  an :doc:`sklearn op<./chariots.sklearn>`
"""
from ._base_op import BaseOp
from ._loadable_op import LoadableOp

__all__ = [
    'LoadableOp',
    'BaseOp'
]
