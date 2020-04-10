import json
from typing import Any

from .. import pipelines


class TestClient(pipelines.AbstractClient):
    """mock up of the client to test a full app without having to create a server"""

    def __init__(self, app: pipelines.Chariots):
        self._test_client = app.test_client()

    def _post(self, route: str, data: Any):
        response = self._test_client.post(route, data=json.dumps(data), content_type='application/json')
        self._check_code(response.status_code)
        return json.loads(response.data.decode('utf-8'))

    def _get(self, route: str, data: Any):
        response = self._test_client.ge(route)
        self._check_code(response.status_code)
        return json.loads(response.data.decode('utf-8'))
