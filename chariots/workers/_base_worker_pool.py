from abc import abstractmethod, ABC
from enum import Enum
from typing import Any

import rq

import chariots


class JobStatus(Enum):
    queued = 'queued'
    running = 'running'
    done = 'done'
    failed = 'failed'
    deferred = 'deferred'

    @classmethod
    def from_rq(cls, status):
        if status == rq.job.JobStatus.QUEUED:
            return cls.queued
        if status == rq.job.JobStatus.FINISHED:
            return cls.done
        if status == rq.job.JobStatus.FAILED:
            return cls.failed
        if status == rq.job.JobStatus.STARTED:
            return cls.running
        if status == rq.job.JobStatus.DEFFERED:
            return cls.deferred
        raise ValueError('unknown job status: {}'.format(status))


class JobResponse(ABC):

    @property
    @abstractmethod
    def status(self) -> JobStatus:
        pass

    @property
    @abstractmethod
    def result(self) -> Any:
        pass


class BaseWorkerPool(ABC):

    @property
    @abstractmethod
    def n_workers(self):
        pass

    @abstractmethod
    def spawn_worker(self):
        pass

    @abstractmethod
    def execute_pipeline_async(self, pipeline: 'chariots.Pipeline', pipeline_input: Any,
                               app: 'chariots.Chariots') -> str:
        pass

    @abstractmethod
    def get_pipeline_response_json_for_id(self, job_id: str):
        pass
