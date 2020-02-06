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


class BaseWorkerPool(ABC):
    """
    `BaseWorkerPool` is the class you will need to subclass in order to make your own JobQueue system
    work with Chariots.

    In order to do so you will need to create:

    * n_workers: a property that informs Chariots of the total number of workers (available as well as in use)
    * spawn_worker: a method that creates a new worker
    * execute_pipeline_async: a method that executes a pipeline inside one of this Pool's workers.
    * get_pipeline_response_json_for_id: a method that to retreieve the json of the `PipelineResponse` if available
    """

    @property
    @abstractmethod
    def n_workers(self):
        """total number of workers in the pool"""
        pass

    @abstractmethod
    def spawn_worker(self):
        """create a new worker in the pool"""
        pass

    @abstractmethod
    def execute_pipeline_async(self, pipeline: 'chariots.Pipeline', pipeline_input: Any,
                               app: 'chariots.Chariots') -> str:
        """
        method to execute a pipeline inside of a worker.

        :param pipeline: the pipeline that needs to be executed inside a worker
        :param pipeline_input: the input to be fed to the pipeline when it gets executed
        :param app: the app that this pipeline belongs to

        :return: the id string of the job. This id needs to correspond to the one that will get sent
                 to `BaseWorkerPool.get_pipeline_response_json_for_id`
        """
        pass

    @abstractmethod
    def get_pipeline_response_json_for_id(self, job_id: str) -> str:
        """
        fetches the results from a pipeline that got executed inside a worker. If the results are not available (not
        done, execution failed, ...), the `PipelineResponse` returned will have the corresponding job status and a None
        value

        :param job_id: the id (as outputted from  `execute_pipeline_async` of the job to fetch results for)

        :return: a jsonified version of the corresponding `PipelineResponse`
        """
        pass
