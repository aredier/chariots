""""RQ implementation of the Workers API"""
import json
from _sha1 import sha1
from typing import Any, Dict, Optional

from redis import Redis
from rq import Queue, Connection, Worker

from chariots._deployment.app import Chariots, PipelineResponse
from chariots.base import BaseRunner
from ._base_worker_pool import BaseWorkerPool, JobStatus


def _inner_pipe_execution(pipeline: 'chariots.Pipeline', pipeline_input: Any, runner: BaseRunner,
                          op_store: 'chariots.OpStore'):
    pipeline.load(op_store)
    res = json.dumps(runner.run(pipeline, pipeline_input))
    pipeline.save(op_store)
    return res


class RQWorkerPool(BaseWorkerPool):
    """
    a worker pool based on the RQ queue job queues. You will need a functionning redis to use this. This
    worker pool will allow you to easily paralellize you Chariots app. You can check
    :doc:`the how to guide on workers<../how_to_guides/workers>` to have more info.

    To use an `RQWorkerPool` with your Chariots app, you can do as such.

    .. testsetup::

        >>> import tempfile
        >>> import shutil
        >>> from chariots.op_store._op_store_client import TestOpStoreClient
        >>> app_path = tempfile.mkdtemp()
        >>> my_pipelines = []
        >>> op_store_client = TestOpStoreClient(app_path)

    .. doctest::

        >>> from redis import Redis
        >>> from chariots import Chariots, workers
        ...
        ...
        >>> app = Chariots(
        ...     my_pipelines,
        ...     op_store_client=op_store_client,
        ...     worker_pool=workers.RQWorkerPool(redis=Redis()),
        ...     use_workers=True,
        ...     import_name='app'
        ... )


    :param redis: the redis connection that will be used by RQ
    :param queue_kwargs: additional keyword arguments that will get passed to the `rq.Queue` object at init
                         be aware that the `connection` and `name` arguments will be overridden.
    """

    def __init__(self, redis: Redis, queue_kwargs: Optional[Dict[str, Any]] = None):
        self._queue_name = 'chariots_workers'
        self._redis = redis
        queue_kwargs = queue_kwargs or {}
        queue_kwargs['connection'] = self._redis
        queue_kwargs['name'] = self._queue_name
        self._queue = Queue(**queue_kwargs)
        self._jobs = {}

    def spawn_worker(self):
        with Connection(self._redis):

            worker = Worker(self._queue_name)
            worker.work()

    def execute_pipeline_async(self, pipeline: 'chariots.Pipeline', pipeline_input: Any, app: Chariots) -> str:
        if not self.n_workers:
            raise ValueError('async job requested but it seems no workers are available')
        rq_job = self._queue.enqueue(_inner_pipe_execution, kwargs={
            'pipeline': pipeline,
            'pipeline_input': pipeline_input,
            'runner': app.runner,
            'op_store_client': app.op_store_client
        })
        chariots_job_id = sha1(rq_job.id.encode('utf-8')).hexdigest()
        self._jobs[chariots_job_id] = rq_job
        return chariots_job_id

    def get_pipeline_response_json_for_id(self, job_id: str) -> str:
        rq_job = self._jobs.get(job_id)
        if rq_job is None:
            raise ValueError('job {} was not found, are you sure you submited it using workers'.format(job_id))
        job_status = JobStatus.from_rq(rq_job.get_status())
        return json.dumps(PipelineResponse(json.loads(rq_job.result) if job_status is JobStatus.done else None,
                                           {}, job_id=job_id, job_status=job_status).json())

    @property
    def n_workers(self):
        return Worker.count(connection=self._redis)
