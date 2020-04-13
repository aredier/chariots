from typing import Optional, Union, List, Any, Dict, Tuple

import yaml

from .pipelines import runners, Pipeline, PipelinesClient, callbacks, PipelinesServer
from .op_store import savers, OpStoreClient, OpStoreServer
from .workers import BaseWorkerPool, RQWorkerPool


class PipelinesConfig:
    _sequential_runner_str = ['SequentialRunner', 'sequential_runner', 'sequential-runner']

    def __init__(self, runner: Optional[Union[str, runners.BaseRunner]] = None, server_host: Optional[str] = None,
                 server_port: Optional[Union[str, int]] = None, pipelines: Optional[List[Pipeline]] = None,
                 pipeline_callbacks: Optional[List[callbacks.PipelineCallback]] = None,
                 import_name: Optional[str] = None):
        self._check_runner(runner)
        self.runner = runner
        self.server_host = server_host
        self.server_port = server_port
        self.pipelines = pipelines or []
        self.pipeline_callbacks = pipeline_callbacks or []
        self.import_name = import_name

    @classmethod
    def _check_runner(cls, runner):
        if isinstance(runner, runners.BaseRunner):
            return
        if not isinstance(runner, str):
            raise TypeError('cannot interpret type {} as runner'.format(type(runner)))
        if runner in cls._sequential_runner_str:
            return
        raise ValueError('runner string {} not understood'.format(runner))

    @property
    def _backend_full_url(self) -> str:
        if self.server_host.startswith('http'):
            return '{}:{}'.format(self.server_host, self.server_port)
        return 'http://{}:{}'.format(self.server_host, self.server_port)

    def get_runner(self) -> runners.BaseRunner:
        if isinstance(self.runner, runners.BaseRunner):
            return self.runner
        if self.runner in ['SequentialRunner', 'sequential_runner', 'sequential-runner']:
            return runners.SequentialRunner()
        raise ValueError('runner string {} not understood'.format(self.runner))

    def get_client(self) -> PipelinesClient:
        return PipelinesClient(backend_url=self._backend_full_url)


class WorkersConfig(object):
    _rq_workers_str = ['RQ', 'rq']

    def __init__(self, use_for_all: bool = False, worker_type: Optional[str] = None,
                 worker_pool_kwargs: Optional[Dict[str, Any]] = None):
        self.use_for_all = use_for_all
        self._check_worker_type(worker_type)
        self.worker_type = worker_type
        self.worker_pool_kwargs = worker_pool_kwargs or {}

    @classmethod
    def _check_worker_type(cls, worker_type: str):
        if worker_type not in {*cls._rq_workers_str}:
            raise ValueError('worker type {} not understood'.format(worker_type))

    def get_worker_pool(self) -> BaseWorkerPool:
        if self.worker_type in self._rq_workers_str:
            return RQWorkerPool(**self.worker_pool_kwargs)
        raise ValueError('worker type {} not understood'.format(self.worker_type))


class OpStoreConfig(object):
    _file_saver_str = ['FileSaver', 'file_saver', 'file-saver']
    _google_cloud_saver_str = ['GoogleStorage', 'google_storage', 'google-storage', 'GoogleStorageSaver',
                               'google_storage_saver', 'google-storage-saver']

    def __init__(self, server_host: Optional[str] = None, server_port: Optional[Union[str, int]] = None,
                 saver_type: Optional[str] = None, saver_kwargs: Optional[Dict[str, Any]] = None,
                 op_store_db_url: Optional[str] = None):
        self.server_host = server_host
        self.server_port = server_port
        self._check_saver_type(saver_type)
        self.saver_type = saver_type
        self.saver_kwargs = saver_kwargs or {}
        self.op_store_db_url = op_store_db_url or 'sqlite:///:memory:'

    @classmethod
    def _check_saver_type(cls, saver_type):
        if saver_type not in {*cls._file_saver_str, *cls._google_cloud_saver_str}:
            raise ValueError('saver type {} not understood'.format(saver_type))

    @property
    def _full_server_url(self) -> str:
        if self.server_host.startswith('http'):
            return '{}:{}'.format(self.server_host, self.server_port)
        return 'http://{}:{}'.format(self.server_host, self.server_port)

    def get_client(self) -> OpStoreClient:
        return OpStoreClient(url=self._full_server_url)

    def get_server(self) -> OpStoreServer:
        return OpStoreServer(
            saver=self.get_saver(),
            db_url=self.op_store_db_url
        )

    def get_saver(self) -> savers.BaseSaver:
        if self.saver_type in self._file_saver_str:
            return savers.FileSaver(**self.saver_kwargs)
        if self.saver_type in self._google_cloud_saver_str:
            return savers.GoogleStorageSaver(**self.saver_kwargs)
        raise ValueError('saver type {} not understood'.format(self.saver_type))


class ChariotsConfig:

    def __init__(self, config_file=None, pipelines_config: Optional[PipelinesConfig] = None,
                 pipelines_worker_config: Optional[WorkersConfig] = None,
                 op_store_config: Optional[OpStoreConfig] = None):
        if config_file is not None:
            self.pipelines_config, self.pipelines_workers_config, self.op_store_config = self._load_file(config_file)
        if pipelines_config is not None:
            self.pipelines_config = pipelines_config
        if pipelines_worker_config is not None:
            self.pipelines_workers_config = pipelines_worker_config
        if op_store_config is not None:
            self.op_store_config = op_store_config

    @staticmethod
    def _load_file(config_path) -> Tuple[PipelinesConfig, WorkersConfig, OpStoreConfig]:
        with open(config_path, 'r') as config_file:
            config = yaml.safe_load(config_file)
        return (
            PipelinesConfig(**config['pipelines']),
            WorkersConfig(**config['pipelines_workers']),
            OpStoreConfig(**config['op_store']),
        )

    def get_pipelines_client(self) -> PipelinesClient:
        return self.pipelines_config.get_client()

    def get_pipelines_server(self) -> PipelinesServer:
        return PipelinesServer(
            op_store_client=self.get_op_store_client(),
            runner=self.pipelines_config.get_runner(),
            default_pipeline_callbacks=self.pipelines_config.pipeline_callbacks,
            worker_pool=self.pipelines_workers_config.get_worker_pool(),
            use_workers=self.pipelines_workers_config.use_for_all,
            app_pipelines=self.pipelines_config.pipelines,
            import_name=self.pipelines_config.import_name
        )

    def get_op_store_server(self) -> OpStoreServer:
        return self.op_store_config.get_server()

    def get_op_store_client(self) -> OpStoreClient:
        return self.op_store_config.get_client()

    def get_pipelines_worker_pool(self) -> BaseWorkerPool:
        return self.pipelines_workers_config.get_worker_pool()
