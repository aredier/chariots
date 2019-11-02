import json
import shutil
import tempfile
from _sha1 import sha1
from typing import Any, Dict, Optional

import rq
from redis import Redis
from rq import Queue, Connection, Worker, get_current_job
from rq.job import Job

from chariots import OpStore
import chariots
from ..app import Chariots, PipelineResponse
from chariots._deployment.workers._base_worker_pool import BaseWorkerPool, JobResponse, JobStatus
from chariots.base import BaseRunner, BaseSaver


def _inner_pipe_execution(pipeline: chariots.Pipeline, pipeline_input: Any, saver: BaseSaver, runner: BaseRunner,
                          op_store: OpStore):
    op_store.reload()
    pipeline.load(op_store)
    pipeline.prepare(saver)
    res = json.dumps(PipelineResponse(runner.run(pipeline, pipeline_input), pipeline.get_pipeline_versions(),
                                      job_id=get_current_job().id, job_status=JobStatus.done).json())
    pipeline.save(op_store)
    op_store.save()
    return res


class RQJobResponse(JobResponse):

    def __init__(self, rq_job: Job):
        self.rq_job =rq_job

    @property
    def status(self) -> JobStatus:
        status = self.rq_job.get_status()
        if status == rq.job.JobStatus.QUEUED:
            return JobStatus.queued
        if status == rq.job.JobStatus.FINISHED:
            return JobStatus.done
        if status == rq.job.JobStatus.FAILED:
            return JobStatus.failed
        if status == rq.job.JobStatus.STARTED:
            return JobStatus.running
        if status == rq.job.JobStatus.DEFFERED:
            return JobStatus.deferred
        raise ValueError('unknown job status: {}'.format(status))

    @property
    def result(self) -> Any:
        return self.rq_job.result


class RQWorkerPool(BaseWorkerPool):

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

    def execute_pipeline_async(self, pipeline: chariots.Pipeline, pipeline_input: Any, app: Chariots) -> str:
        if not self.n_workers:
            raise ValueError('async job requested but it seems no workers are available')
        rq_job = self._queue.enqueue(_inner_pipe_execution, kwargs={
            "pipeline": pipeline,
            "pipeline_input": pipeline_input,
            "saver": app.saver,
            "runner": app.runner,
            "op_store": app.op_store
        })
        chariots_job_id = sha1(rq_job.id.encode('utf-8')).hexdigest()
        self._jobs[chariots_job_id] = rq_job
        return chariots_job_id

    def get_pipeline_response_json_for_id(self, job_id: str):
        rq_job = self._jobs.get(job_id)
        if rq_job is None:
            raise ValueError('job {} was not found, are you sure you submited it using workers'.format(job_id))
        job_status = JobStatus.from_rq(rq_job.get_status())
        if job_status is not JobStatus.done:
            return json.dumps(PipelineResponse(None, {}, job_id=job_id, job_status=job_status).json())
        return rq_job.result

    @property
    def n_workers(self):
        return Worker.count(connection=self._redis)
