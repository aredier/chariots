# -*- coding: utf-8 -*-

"""Top-level package for chariots."""

from ._pipeline import Pipeline
from ._deployment.client import Client
from ._deployment.client import TestClient
from ._deployment.app import Chariot
from ._ml_mode import MLMode
from ._op_store import OpStore

__author__ = """Antoine Redier"""
__email__ = 'antoine.redier2@gmail.com'
__version__ = '0.1.2'

__all__ = [
    "Pipeline",
    "Chariot",
    "Client",
    "OpStore",
    "MLMode",
    "TestClient",
]
