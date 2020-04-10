"""
The OpStore is the Component of Chariots that handles persisting and reloading Operations. It also
handles keeping track of the different versions of all the Ops that it registers.

As a Chariots user, you will need to setup the OpStore Server (using the OpStoreServer class) and
than interact with the OpStore Client. Alternatively You can pass the OpStore client to your Chariots
App and use this instead.
"""

from . import savers
from ._op_store_client import OpStoreClient, BaseOpStoreClient
from ._op_store import OpStoreServer

__all__ = [
    'OpStoreServer',
    'OpStoreClient',
    'savers',
    'BaseOpStoreClient'
]
