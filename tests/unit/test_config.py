import os

import pytest
import yaml

from chariots import config, workers, op_store
from chariots.pipelines import runners, PipelinesClient


def test_pipelines_config(basic_sk_pipelines):
    """tests the basic configuration is respected"""
    train, predict = basic_sk_pipelines
    runner = 'sequential-runner'
    server_port = 80
    import_name = 'test_chariots_pipelines_server'
    test_config = config.PipelinesConfig(
        runner=runner,
        server_host='localhost',
        server_port=server_port,
        pipelines=[train, predict],
        pipeline_callbacks=[],
        import_name=import_name,
    )

    assert test_config.runner == runner
    assert test_config.server_port == server_port
    assert test_config.pipelines == [train, predict]
    assert test_config.pipeline_callbacks == []
    assert test_config.import_name == import_name

    test_runner = test_config.get_runner()
    assert isinstance(test_runner, runners.SequentialRunner)

    test_client = test_config.get_client()
    assert isinstance(test_client, PipelinesClient)


def test_pipelines_config_str_runner():
    """tests pipelines config when the runner is specified with a string (normal cases and errors"""
    config_dict = {
        'server_host': 'localhost',
        'server_port': 80,
    }

    # testing sequential runner
    for runner_str in ['SequentialRunner', 'sequential-runner', 'sequential_runner']:
        config_dict['runner'] = runner_str
        assert isinstance(config.PipelinesConfig(**config_dict).get_runner(), runners.SequentialRunner)

    test_runner = runners.SequentialRunner()
    config_dict['runner'] = test_runner
    assert config.PipelinesConfig(**config_dict).get_runner() == test_runner

    config_dict['runner'] = 'wrong_runner_string'
    with pytest.raises(ValueError):
        config.PipelinesConfig(**config_dict)


def test_pipelines_config_client():
    """tests the creation of a client (mostly the url string creation"""
    test_config = config.PipelinesConfig(server_host='localhost', server_port=80, runner='sequential_runner')
    test_client = test_config.get_client()
    assert isinstance(test_client, PipelinesClient)
    assert test_client.backend_url == 'http://localhost:80'

    test_config = config.PipelinesConfig(server_host='http://localhost', server_port=5000, runner='sequential_runner')
    test_client = test_config.get_client()
    assert isinstance(test_client, PipelinesClient)
    assert test_client.backend_url == 'http://localhost:5000'


def test_workers_config():
    """tests the basic config of the workers config"""
    use_for_all = True
    worker_type = 'rq'

    test_pool_config = config.WorkersConfig(use_for_all=use_for_all, worker_type=worker_type)

    assert test_pool_config.use_for_all == use_for_all
    assert test_pool_config.worker_type == worker_type

    test_pool = test_pool_config.get_worker_pool()
    assert isinstance(test_pool, workers.RQWorkerPool)


def test_workers_config_worker_type_str():
    """tests the worker str customization and fail caseses"""
    config_dict = {
        'use_for_all': False
    }

    for type_str in ['RQ', 'rq']:
        config_dict['worker_type'] = type_str
        test_config = config.WorkersConfig(**config_dict)
        assert isinstance(test_config.get_worker_pool(), workers.RQWorkerPool)

    config_dict['worker_type'] = 'wrong-config-str'
    with pytest.raises(ValueError):
        config.WorkersConfig(**config_dict)


def test_worker_pool_kwargs():
    host = 'localhost'
    port = 5000
    config_dict = {
        'use_for_all': False,
        'worker_type': 'rq',
        'worker_pool_kwargs': {
            'redis_kwargs': {
                'host': host,
                'port': port
            }
        }
    }
    test_config = config.WorkersConfig(**config_dict)
    worker_pool = test_config.get_worker_pool()
    assert worker_pool._redis.connection_pool.connection_kwargs['host'] == host
    assert worker_pool._redis.connection_pool.connection_kwargs['port'] == port


def test_op_store_basic():
    """ tests the basic confifguration of the op_store"""
    host = 'localhost'
    port = 5000
    saver_type = 'file_saver'
    db_url = 'sqlite:///memory'
    test_op_store_config = config.OpStoreConfig(
        server_host=host,
        server_port=port,
        saver_type=saver_type,
        op_store_db_url=db_url,
        saver_kwargs={'root_path': '/tmp/op_store'}
    )

    assert test_op_store_config.server_host == host
    assert test_op_store_config.server_port == port
    assert test_op_store_config.saver_type == saver_type
    assert test_op_store_config.op_store_db_url == db_url

    assert isinstance(test_op_store_config.get_client(), op_store.OpStoreClient)
    assert isinstance(test_op_store_config.get_server(), op_store.OpStoreServer)
    assert isinstance(test_op_store_config.get_saver(), op_store.savers.FileSaver)


def test_op_store_saver_type_str():
    """ tests the configuration of op_store saver (and fail cases)"""
    config_dict = {
        'server_host': 'localhost',
        'server_port': 5000,
        'op_store_db_url': 'sqlite:///memory:'
    }

    for file_saver_str in ['FileSaver', 'file-saver', 'file_saver']:
        root_path = '/tmp/op_store'
        config_dict['saver_type'] = file_saver_str
        config_dict['saver_kwargs'] = {'root_path': root_path}
        test_config = config.OpStoreConfig(**config_dict)
        for test_saver in [test_config.get_saver(), test_config.get_server()._saver]:
            assert isinstance(test_saver, op_store.savers.FileSaver)
            assert test_saver.root_path == root_path

    # cannot test google because credentials needed for test

    # for google_saver_str in ['GoogleStorage', 'google-storage', 'GoogleStorageSaver', 'google-storage-saver']:
    #     root_path = 'op_store'
    #     bucket_name = 'test_bucket'
    #     config_dict['saver_type'] = google_saver_str
    #     config_dict['saver_kwargs'] = {'root_path': root_path, 'bucket_name': bucket_name}
    #     test_config = config.OpStoreConfig(**config_dict)
    #     for test_saver in [test_config.get_saver(), test_config.get_server()._saver]:
    #         assert isinstance(test_saver, op_store.savers.GoogleStorageSaver)
    #         assert test_saver.root_path == root_path
    #         assert test_saver._bucket_name == bucket_name


def test_saver_kwargs():
    """test the customization of the saver used by the op store"""
    path = '/tmp/test_path'
    config_dict = {
        'server_host': 'localhost',
        'server_port': 5000,
        'op_store_db_url': 'sqlite:///memory:',
        'saver_type': 'file-saver',
        'saver_kwargs': {
            'root_path': path
        }
    }
    test_config = config.OpStoreConfig(**config_dict)
    assert test_config.get_saver().root_path == path


@pytest.fixture
def full_config():
    return {
        'pipelines': {
            'runner': 'sequential-runner',
            'server_host': 'localhost',
            'server_port': 80,
            'import_name': 'test_chariots_pipelines_server'
        },
        'pipelines_workers': {
            'worker_type': 'rq',
            'use_for_all': True,
            'worker_pool_kwargs': {
                'redis_kwargs': {
                    'host': 'localhost',
                    'port': 8080
                }
            }
        },
        'op_store': {
            'server_host': 'localhost',
            'server_port': 5000,
            'op_store_db_url': 'sqlite:///memory:',
            'saver_type': 'file-saver',
            'saver_kwargs': {
                'root_path': '/tmp/op_store_test'
            }
        }
    }


def do_full_config_test(chariots_config: config.ChariotsConfig):
    test_op_store_client = chariots_config.get_op_store_client()
    assert test_op_store_client.url == 'http://localhost:5000'

    test_op_store_server = chariots_config.get_op_store_server()
    assert isinstance(test_op_store_server._saver, op_store.savers.FileSaver)
    assert test_op_store_server._saver.root_path == '/tmp/op_store_test'

    test_worker_pool = chariots_config.get_pipelines_worker_pool()
    assert isinstance(test_worker_pool, workers.RQWorkerPool)
    assert test_worker_pool._redis.connection_pool.connection_kwargs['host'] == 'localhost'
    assert test_worker_pool._redis.connection_pool.connection_kwargs['port'] == 8080

    test_pipelines_client = chariots_config.get_pipelines_client()
    assert isinstance(test_pipelines_client, PipelinesClient)
    assert test_pipelines_client.backend_url == 'http://localhost:80'

    test_pipelines_server = chariots_config.get_pipelines_server()
    assert isinstance(test_pipelines_server.runner, runners.SequentialRunner)
    assert isinstance(test_pipelines_server.op_store_client, op_store.OpStoreClient)
    assert test_pipelines_server.op_store_client.url == 'http://localhost:5000'
    assert test_pipelines_server.use_workers
    assert isinstance(test_pipelines_server._worker_pool, workers.RQWorkerPool)
    assert test_pipelines_server._worker_pool._redis.connection_pool.connection_kwargs['host'] == 'localhost'
    assert test_pipelines_server._worker_pool._redis.connection_pool.connection_kwargs['port'] == 8080


def test_chariots_config_basic(full_config):
    """ tests the basic setup and usage of the chariots config"""
    chariots_config = config.ChariotsConfig(
        pipelines_config=config.PipelinesConfig(**full_config['pipelines']),
        pipelines_worker_config=config.WorkersConfig(**full_config['pipelines_workers']),
        op_store_config=config.OpStoreConfig(**full_config['op_store'])
    )
    do_full_config_test(chariots_config)


def test_chariots_yaml_config(tmpdir, full_config):
    """ tests loading part of the config using a yaml file"""
    path = os.path.join(str(tmpdir), 'config.yaml')
    with open(path, 'w') as config_file:
        yaml.dump(full_config, config_file)

    chariots_config = config.ChariotsConfig(config_file=path)
    do_full_config_test(chariots_config)
