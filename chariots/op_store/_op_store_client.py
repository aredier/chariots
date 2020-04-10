"""module for the `OpStore` class that handles saving op's data at the right place"""
import abc
import base64
import json
from typing import Text, Set, Optional

import requests

from .. import versioning, pipelines


class BaseOpStoreClient(abc.ABC):
    """base class for op store clients"""

    @abc.abstractmethod
    def post(self, route, arguments_json):
        """posts request the backend"""

    def get_all_versions_of_op(self, desired_op: 'pipelines.ops.BaseOp') -> Optional[Set[versioning.Version]]:
        """
        returns all the available versions of an op ever persisted in the OpGraph (or any Opgraph using the same
        _meta.json)

        :param desired_op: the op to get the previous persisted versions
        """
        response_json = self.post('/v1/get_all_versions_of_op', {'desired_op_name': desired_op.name})
        if not response_json:
            return None
        return {versioning.Version.parse(version_string) for version_string in response_json['all_versions']}

        # all_versions = [
        #     versions
        #     for downstream_data in self._all_op_links.values()
        #     for upstream_op, versions in downstream_data.items()
        #     if upstream_op == desired_op.name
        # ]

    def get_validated_links(self, downstream_op_name: Text,
                            upstream_op_name: Text) -> Optional[Set[versioning.Version]]:
        """
        gets all the validated links (versions that works) between an upstream op and a downstream op (if none
        exist, `None` is returned)
        """
        response_json = self.post('/v1/get_validated_links', {
            'downstream_op_name': downstream_op_name,
            'upstream_op_name': upstream_op_name
        })

        return {
            versioning.Version.parse(version_string) for version_string in response_json['upstream_versions']
        } if response_json else None

        # return self._all_op_links.get(downstream_op_name, {}).get(upstream_op_name)

    def get_op_bytes_for_version(self, desired_op: 'pipelines.ops.BaseOp', version: versioning.Version) -> bytes:
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

    def save_op_bytes(self, op_to_save: 'pipelines.ops.BaseOp', version: versioning.Version, op_bytes: bytes):
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
                            upstream_op_version: versioning.Version):
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

    def pipeline_exists(self, pipeline_name: str) -> bool:
        """
        checks if a pipeline is already registered in the OpStore

        :param pipeline_name: the name of the pipeline to check
        :return: a boolean (True if the pipeline exists)
        """

        return self.post('/v1/pipeline_exists', {'pipeline_name': pipeline_name})['exists']

    def register_new_pipeline(self, pipeline: 'pipelines.Pipeline'):
        """
        registers a new pipeline to register to the Store (this will only update the `db_pipeline` table of the db so
        you will need to save each of your Ops and their validated links if using manually

        :param pipeline: the pipeline to register
        """

        for upstream_node, downstream_node in pipeline.get_all_op_links():
            if downstream_node is None:
                self.post('/v1/register_new_pipeline', {
                    'pipeline_name': pipeline.name,
                    'last_op_name': upstream_node.name
                })
                return
        raise ValueError('did not manage to find last node of the pipeline')


class OpStoreClient(BaseOpStoreClient):
    """
    Client used to query the OpStoreServer.
    """

    def __init__(self, url):
        self.url = url

    def post(self, route, arguments_json):
        response = requests.post(
            route,
            headers={'Content-Type': 'application/json'},
            data=json.dumps(arguments_json)
        )
        if response.status_code != 200:
            raise ValueError('something went wrong')

        return response.json()
