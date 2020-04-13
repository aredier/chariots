"""
The config module allows you to define all of your Chariot's app configuration in one place. Once this is done, the
ChariotsConfiguration will allow you to

* create all of the necessary servers, workers and have them all work together
* get the clients you want to interact with.

The easiest way to create your config is to use a yaml file with all of the "static" configuration that don't need to
be created programmatically. here is an example::

    op_store:
      op_store_db_url: 'sqlite:///memory:'
      saver_kwargs:
        root_path: /tmp/op_store_test
      saver_type: file-saver
      server_host: localhost
      server_port: 5000
    pipelines:
      import_name: test_chariots_pipelines_server
      runner: sequential-runner
      server_host: localhost
      server_port: 80
    pipelines_workers:
      use_for_all: true
      worker_pool_kwargs:
        redis_kwargs:
          host: localhost
          port: 8080
      worker_type: rq

Once this is done, you can load the configuration using the `ChariotsConfig` class:

    .. testsetup::

        >>> from chariots.config import ChariotsConfig

    .. doctest::

        >>> my_config = ChariotsConfiguration('config.yaml')  # doctest: +SKIP

Once you have loaded the configuration, you can always modify it programatically:

    .. testsetup::

        >>> pipelines_kwargs = {'runner': 'sequential-runner'}
        >>> pipelines_workers_kwargs = {'worker_type': 'rq'}
        >>> op_store_kwargs = {'saver_type': 'file-saver'}
        >>> my_config = ChariotsConfig(
        ...     pipelines_config=PipelinesConfig(**pipelines_kwargs),
        ...     pipelines_worker_config=WorkersConfig(**pipelines_workers_kwargs),
        ...     op_store_config=OpStoreConfig(**op_store_kwargs),
        ... )
        >>> some_pipeline = None

    .. doctest::

        >>> my_config.pipelines_config.pipelines.append(some_pipeline)


You can than get the clients or servers you want to deploy/use:

    .. doctest::

        >>> my_config.get_pipelines_server() # doctest: +SKIP
        >>> my_config.get_op_store_client() # doctest: +SKIP

The ChariotsConfig is built around three main components:

* The pipelines config
* The op_store config
* The workers config
"""
from typing import Optional, Union, List, Any, Dict, Tuple

import yaml

from .pipelines import runners, Pipeline, PipelinesClient, callbacks, PipelinesServer
from .op_store import savers, OpStoreClient, OpStoreServer
from .workers import BaseWorkerPool, RQWorkerPool


class PipelinesConfig:
    """Configuration of the pipelines. This mainly describes what and how your pipelines should be run and served"""
    _sequential_runner_str = ['SequentialRunner', 'sequential_runner', 'sequential-runner']

    def __init__(self, runner: Optional[Union[str, runners.BaseRunner]] = None,   # pylint: disable=too-many-arguments
                 server_host: Optional[str] = None, server_port: Optional[Union[str, int]] = None,
                 pipelines: Optional[List[Pipeline]] = None,
                 pipeline_callbacks: Optional[List[callbacks.PipelineCallback]] = None,
                 import_name: Optional[str] = None):
        """
        :param runner: the runner to use to run the instance (both in the main server and in the workers. This should
                       either be A BaseRunner instance or a string describing the type of runner to use (
                       'sequential-runner' for instance
        :param server_host: the host of the server (mainly used to create the proper client)
        :param server_port: the port to run the server at
        :param pipelines: the pipelines to be present in the pipelines server (this cannot be filled using the config
                          yaml file format and has to be added after loading the file (or at init)
        :param pipeline_callbacks: the callbacks to be used accross all the pipelines of the server. This parameter
                                   cannot be filled trough the config file and have to be filed programmatically.
        :param import_name: the import name of the resulting PipelinesServer
        """
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
        """creates a runner according to the configuration"""
        if isinstance(self.runner, runners.BaseRunner):
            return self.runner
        if self.runner in ['SequentialRunner', 'sequential_runner', 'sequential-runner']:
            return runners.SequentialRunner()
        raise ValueError('runner string {} not understood'.format(self.runner))

    def get_client(self) -> PipelinesClient:
        """creates a PipelinesClient configured according to this configuration"""
        return PipelinesClient(backend_url=self._backend_full_url)


class WorkersConfig:  # pylint: disable=too-few-public-methods
    """the configuration of Chariots Workerpolls"""
    _rq_workers_str = ['RQ', 'rq']

    def __init__(self, use_for_all: bool = False, worker_type: Optional[str] = None,
                 worker_pool_kwargs: Optional[Dict[str, Any]] = None):
        """
        :param use_for_all: whether or not all the pipelines should be executed asynchronously using workers
        :param worker_type: the type of worker pool to use. This should be a string such as 'rq' for instance
        :param worker_pool_kwargs: additional keyword arguments to be passed down to the init of the WorkerPool
        """
        self.use_for_all = use_for_all
        self._check_worker_type(worker_type)
        self.worker_type = worker_type
        self.worker_pool_kwargs = worker_pool_kwargs or {}

    @classmethod
    def _check_worker_type(cls, worker_type: str):
        if worker_type not in {*cls._rq_workers_str}:
            raise ValueError('worker type {} not understood'.format(worker_type))

    def get_worker_pool(self) -> BaseWorkerPool:
        """get the WorkerPool corresponding to this configuration"""
        if self.worker_type in self._rq_workers_str:
            return RQWorkerPool(**self.worker_pool_kwargs)
        raise ValueError('worker type {} not understood'.format(self.worker_type))


class OpStoreConfig:
    """configuration for the Op Store server and client"""
    _file_saver_str = ['FileSaver', 'file_saver', 'file-saver']
    _google_cloud_saver_str = ['GoogleStorage', 'google_storage', 'google-storage', 'GoogleStorageSaver',
                               'google_storage_saver', 'google-storage-saver']

    def __init__(self, server_host: Optional[str] = None,  # pylint: disable=too-many-arguments
                 server_port: Optional[Union[str, int]] = None, saver_type: Optional[str] = None,
                 saver_kwargs: Optional[Dict[str, Any]] = None, op_store_db_url: Optional[str] = None):
        """
        :param server_host: the host of the server (where the client should try to contact)
        :param server_port: the port the server should be run at
        :param saver_type: the type of saver to be used by the op store to save the serialized ops (this should be a
                           string describing the saver type such as 'file-saver' or 'google-storage-saver'
        :param saver_kwargs: additional keyword arguments to be used when instanciating the saver
        :param op_store_db_url: the url (sqlalchemy compatibale) to locate the Op Store database
        """
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
        """returns the op_store client as configured by this OpStoreConfig"""
        return OpStoreClient(url=self._full_server_url)

    def get_server(self) -> OpStoreServer:
        """returns the op_store server as configured by this OpStoreConfig"""
        return OpStoreServer(
            saver=self.get_saver(),
            db_url=self.op_store_db_url
        )

    def get_saver(self) -> savers.BaseSaver:
        """returns the op_store saver as configured by this OpStoreConfig"""
        if self.saver_type in self._file_saver_str:
            return savers.FileSaver(**self.saver_kwargs)
        if self.saver_type in self._google_cloud_saver_str:
            return savers.GoogleStorageSaver(**self.saver_kwargs)
        raise ValueError('saver type {} not understood'.format(self.saver_type))


class ChariotsConfig:
    """
    full configuration for Chariots. This configuration encapsulates all the other configurations available in Chariots
    you can either instanciate your Chariots Config using a yaml file.:

    .. testsetup::
        >>> from chariots.config import ChariotsConfig
        >>> pipelines_kwargs = {'runner': 'sequential-runner'}
        >>> pipelines_workers_kwargs = {'worker_type': 'rq'}
        >>> op_store_kwargs = {'saver_type': 'file-saver'}

    .. doctest::

        >>> my_conf = ChariotsConfig('config.yaml')  # doctest: +SKIP

    or you can instantite it in pure python:

    ..doctest::

        >>> my_conf = ChariotsConfig(
        ...     pipelines_config=PipelinesConfig(**pipelines_kwargs),
        ...     pipelines_worker_config=WorkersConfig(**pipelines_workers_kwargs),
        ...     op_store_config=OpStoreConfig(**op_store_kwargs),
        ... )
    """

    def __init__(self, config_file=None, pipelines_config: Optional[PipelinesConfig] = None,
                 pipelines_worker_config: Optional[WorkersConfig] = None,
                 op_store_config: Optional[OpStoreConfig] = None):
        """
        :param config_file: a file to load the configuration from. If any other arguments are set during the
                            instantiation, the config in the file that portrays to this part of the configuration will
                            be completely overridden (for instance if both `config_file` and `pipelines_config` are set,
                            the configuration under pipelines of the config file will be ignored)
        :param pipelines_config: the pipelines specific configuration.
        :param pipelines_worker_config: the pipelines workers configuration
        :param op_store_config: the pipelines Op Store configuration
        """
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
        """gets the pipelines client as configured by this configuration"""
        return self.pipelines_config.get_client()

    def get_pipelines_server(self) -> PipelinesServer:
        """gets the pipelines server as configured by this configuration"""
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
        """gets the Op Store server as configured by this configuration"""
        return self.op_store_config.get_server()

    def get_op_store_client(self) -> OpStoreClient:
        """gets the op store client as configured by this configuration"""
        return self.op_store_config.get_client()

    def get_pipelines_worker_pool(self) -> BaseWorkerPool:
        """gets the pipelines workers as configured by this configuration"""
        return self.pipelines_workers_config.get_worker_pool()
