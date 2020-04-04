"""module for the `OpStore` class that handles saving op's data at the right place"""
import abc
import base64
import copy
import json
import os
from typing import Union, Dict, Text, Set, Optional, List

import requests
from sqlalchemy.orm import aliased

from chariots import base  # pylint: disable=unused-import;# noqa
from chariots.op_store._op_store import OpStoreServer
from chariots.savers import FileSaver
from chariots.versioning import Version

# TODO test db

class BaseOpStoreClient(abc.ABC):
    """
    A Chariots OpStore handles the persisting of Ops and their versions as well as the accepted versions of each op's
    inputs.

   the OpStore persists all this metadata about persisted ops in the `/_meta.json` file using the saver provided at init

   all the serialized ops are saved at /models/<op name>/<version>

    The OpStore is mostly used by the Pipelines and the nodes at saving time to:

    - persist the ops that they have updated
    - register new versions
    - register links between different ops and different versions that are valid (for instance this versions of the PCA
      is valid for this new version of the RandomForest

    and at loading time to:

    - check latest available version of an op
    - check if this version is valid with the rest of the pipeline
    - recover the bytes of the latest version if it is valid

    the OpStore identifies op's by there name (usually a snake case of the Class of your op) so changing this name
    (or changing the class name) might make it hard to recover the metadata and serialized bytes of the Ops

    :param saver: the saver the op_store will use to retrieve it's metadata and subsequent ops
    """

    @abc.abstractmethod
    def post(self, route, arguments_json):
        pass

    # def _load_from_saver(self) -> _OpGraph:
    #     """
    #     loads and parses all the versions from the meta json
    #     """
    #     try:
    #         mapping = json.loads(self._saver.load(path=self._location).decode('utf-8'))
    #         return self._parse_mapping(mapping)
    #     except FileNotFoundError:
    #         return {}

    def reload(self):
        """reloads the op data from the saver"""
        # TODO delete
        return
        # self._all_op_links = self._load_from_saver()
    #
    # def _parse_mapping(
    #         self, op_version_json: Union[Dict[Text, Dict[Text, Set[Text]]], Dict[Text, Set[Text]]]
    # ) -> Union[Dict[Text, Dict[Text, Set[Version]]], Dict[Text, Set[Version]]]:
    #     """
    #     parses the saved meta and returns a valid metadata (with versions as object rather than strings)
    #
    #     :param op_version_json: all the op version metadata in json format
    #     :return: the op version metadata
    #     """
    #     for key, value in op_version_json.items():
    #         if isinstance(value, dict):
    #             op_version_json[key] = self._parse_mapping(value)
    #         if isinstance(value, list):
    #             op_version_json[key] = {Version.parse(version_str) for version_str in value}
    #     return op_version_json

    def save(self):
        """
        persists all the metadata about ops and versions available in the store using the store's saver.

        The saved metadata can be found at `/_meta.json` from the saver's route.
        """
        # TODO delete

        return

        # version_dict_with_str_versions = {
        #     downstream_op_name: {
        #         upstream_op_name: [str(version) for version in upstream_data]
        #         for upstream_op_name, upstream_data in downstream_data.items()
        #     }
        #     for downstream_op_name, downstream_data in self._all_op_links.items()
        # }
        # self._saver.save(json.dumps(version_dict_with_str_versions).encode('utf-8'), path=self._location)

    def get_all_versions_of_op(self, desired_op: 'base.BaseOp') -> Optional[Set[Version]]:
        """
        returns all the available versions of an op ever persisted in the OpGraph (or any Opgraph using the same
        _meta.json)

        :param desired_op: the op to get the previous persisted versions
        """
        response_json = self.post('/v1/get_all_versions_of_op', {'desired_op_name': desired_op.name})
        if not response_json:
            return None
        return {Version.parse(version_string) for version_string in response_json['all_versions']}

        # all_versions = [
        #     versions
        #     for downstream_data in self._all_op_links.values()
        #     for upstream_op, versions in downstream_data.items()
        #     if upstream_op == desired_op.name
        # ]

    def get_validated_links(self, downstream_op_name: Text, upstream_op_name: Text) -> Optional[Set[Version]]:
        """
        gets all the validated links (versions that works) between an upstream op and a downstream op (if none
        exist, `None` is returned)
        """
        response_json = self.post('/v1/get_validated_links', {
            'downstream_op_name': downstream_op_name,
            'upstream_op_name': upstream_op_name
        })

        return {
            Version.parse(version_string) for version_string in response_json['upstream_versions']
        } if response_json else None

        # return self._all_op_links.get(downstream_op_name, {}).get(upstream_op_name)

    def get_op_bytes_for_version(self, desired_op: 'base.BaseOp', version: Version) -> bytes:
        """
        loads the persisted bytes of op for a specific version

        :param desired_op: the op that needs to be loaded
        :param version: the version of the op to load
        :return: the bytes of the op
        """

        response_json = self.post('/v1/get_op_bytes_for_version', {
            'desired_op_name': desired_op.name,
            'version': str(version)
        })

        return base64.b64decode(response_json['bytes'].encode('utf-8'))

    def save_op_bytes(self, op_to_save: 'base.BaseOp', version: Version, op_bytes: bytes):
        """
        saves op_bytes of a specific op to the path /models/<op name>/<version>.

        the version that is used here is the node version (and not the op_version) as nodes might be able to modify
        some behaviors of the versioning of their underlying op

        :param op_to_save: the op that needs to be saved (this will not be saved as is - only the bytes)
        :param version: the exact version to be used when persisting
        :param op_bytes: the bytes of the op to save that will be persisted
        """

        self.post('/v1/save_op_bytes', {
            'op_name': op_to_save.name,
            'version': str(version),
            'bytes': base64.b64encode(op_bytes).decode('utf-8'),
        })

    def register_valid_link(self, downstream_op_name: Optional[str], upstream_op_name: 'str',
                            upstream_op_version: Version):
        """
        registers a link between an upstream and a downstream op. This means that in future relaods the downstream op
        will whitelist this version for this upstream op

        :param downstream_op_name: the op that needs to whitelist one of it's inputs' new version
        :param upstream_op_name: the op that is getting whitelisted as one of the inputs of the downstream op
        :param upstream_op_version: the valid version of the op that is getting whitelisted
        :return:
        """

        self.post('/v1/register_valid_link', {
            'downstream_op_name': downstream_op_name,
            'upstream_op_name': upstream_op_name,
            'upstream_op_version': str(upstream_op_version)
        })

        # self._all_op_links.setdefault(
        #     downstream_op if downstream_op is not None else '__end_of_pipe__', {}
        # ).setdefault(upstream_op, set()).add(upstream_op_version)

    def pipeline_exists(self, pipeline_name):

        return self.post('/v1/pipeline_exists', {'pipeline_name': pipeline_name})['exists']

    def register_new_pipeline(self, pipeline):

        for upstream_node, downstream_node in pipeline.get_all_op_links():
            if downstream_node is None:
                self.post('/v1/register_new_pipeline', {
                    'pipeline_name': pipeline.name,
                    'last_op_name': upstream_node.name
                })
                return
        raise ValueError('did not manage to find last node of the pipeline')


class OpStoreClient(BaseOpStoreClient):

    def __init__(self, url):
        self.url = url

    def post(self, route, arguments_json):
        response = requests.post(
            route,
            headers={'Content-Type': 'application/json'},
            data=json.dumps(arguments_json)
        )
        if response.status_code != 200:
            # TODO propagate error
            raise ValueError('something went wrong')

        return response.json()


class TestOpStoreClient(BaseOpStoreClient):

    def __init__(self, path, saver=None):
        self.db_path = os.path.join(path, 'db.sqlite')
        ops_path = os.path.join(path, 'ops')
        os.makedirs(ops_path, exist_ok=True)
        self._saver = saver or FileSaver(ops_path)
        self.server = OpStoreServer(self._saver, db_url='sqlite:///{}'.format(self.db_path))
        self._test_client = self.server.flask.test_client()

    def post(self, route, arguments_json):
        response = self._test_client.post(route, data=json.dumps(arguments_json), content_type='application/json')
        if response.status_code != 200:
            # TODO propagate error
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
        self.server = OpStoreServer(self._saver, 'sqlite:///{}'.format(self.db_path))
        self._test_client = self.server.flask.test_client()
