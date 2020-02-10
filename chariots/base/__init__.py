"""
The Base Module Gathers all the main classes in the Chariots framework that can be subclassed to create custom
behaviors:

- creating new Ops for preprocessing and feature extraction (subclassing `BaseOp`)
- supporting new ML frameworks with `BaseMLOp`
- creating a custom node (ABTesting, ...) with the `BaseNode`
- changing the execution behavior of pipelines (Multiprocessing, cluster computing, ...) with `BaseRunner`
- saving your ops and metadata to a different cloud provider with `BaseSaver`
- creating new serialisation formats for datasets and models with `BaseSerializer`

"""
from ._base_nodes import BaseNode
from ._base_op import BaseOp
from ._base_ml_op import BaseMLOp
from ._base_runner import BaseRunner
from ._base_saver import BaseSaver
from ._base_serializer import BaseSerializer


__all__ = [
    'BaseOp',
    'BaseMLOp',
    'BaseRunner',
    'BaseSaver',
    'BaseSerializer',
    'BaseNode'
]
