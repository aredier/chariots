from abc import abstractmethod, ABC
from enum import Enum
from typing import Any

import rq

import chariots


class JobStatus(Enum):
    """
    enum of all the possible states a job can be in.
    """
    queued = 'queued'
    running = 'running'
    done = 'done'
    failed = 'failed'
    deferred = 'deferred'

    @classmethod
    def from_rq(cls, status: rq.job.JobStatus) -> 'JobStatus':
        """
        Translates an RQ Job status into a Chariots JobStatus
        """
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
    """
    Response output produced once a Job. You can get the `status` and the `result` (when available)
    of the job.
    """

    @property
    @abstractmethod
    def status(self) -> JobStatus:
        """
        the status of the Job in question.
        """
        pass

    @property
    @abstractmethod
    def result(self) -> Any:
        """
        the results of the Job at hand.
        """
        pass


class BaseWorkerPool(ABC):
    """
    `BaseWorkerPool` is the class you will need to subclass in order to make your own JobQueue system
    work with Chariots.

    In order to do so you will need to create:
    - n_workers: a property that informs Chariots of the total number of workers (available as well as in use)
    - spawn_worker: a method that creates a new worker
    - execute_pipeline_async: a method that executes a pipeline inside one of this Pool's workers.
    - get_pipeline_response_json_for_id: a method that is available to retreieve
    """

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
