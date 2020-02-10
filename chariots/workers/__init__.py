"""
module that handles Workers in your Chariots. those allow you to:
* execute pipelines in parallel
* execute pipelines asynchronously (not blocking the main server process)

This module also provides a default implementation using RQ
"""
from ._base_worker_pool import BaseWorkerPool, JobStatus
from ._rq_worker_pool import RQWorkerPool

__all__ = [
    'BaseWorkerPool',
    'RQWorkerPool',
    'JobStatus'
]
