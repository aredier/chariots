# pylint: disable=missing-module-docstring
import copy
import json
import os

from .. import op_store


class TestOpStoreClient(op_store.BaseOpStoreClient):
    """helper class to have a client without launching the server"""

    def __init__(self, path, saver=None):
        self.db_path = os.path.join(path, 'db.sqlite')
        ops_path = os.path.join(path, 'ops')
        os.makedirs(ops_path, exist_ok=True)
        self._saver = saver or op_store.savers.FileSaver(ops_path)
        self.server = op_store.OpStoreServer(self._saver, db_url='sqlite:///{}'.format(self.db_path))
        self._test_client = self.server.flask.test_client()

    def post(self, route, arguments_json):
        response = self._test_client.post(route, data=json.dumps(arguments_json), content_type='application/json')
        if response.status_code != 200:
            raise ValueError('something went wrong')

        return json.loads(response.data.decode('utf-8'))

    def __getstate__(self):
        server = self.server
        _test_client = self._test_client
        res = self.__dict__
        res['_test_client'] = None
        res['server'] = None
        res = copy.deepcopy(res)
        self.server = server
        self._test_client = _test_client
        return res

    def __setstate__(self, state):
        self.__dict__ = state
        self.server = op_store.OpStoreServer(self._saver, 'sqlite:///{}'.format(self.db_path))
        self._test_client = self.server.flask.test_client()
