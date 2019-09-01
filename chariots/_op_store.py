import json
from typing import Union, Dict, Text, Set, Any, Optional, List

from chariots import base
from chariots.versioning import Version

_OpGraph = Dict[Text, Dict[Text, Set[Version]]]


class OpStore:
    """
    The op store abstracts the saving of ops and their relevant versions.
    it stores the valid edges of the op graph (links between two ops that work together)
    """

    # the format of the app's op's graoh:
    # {
    #     downstream_op_name: {
    #         upstream_op_name: {
    #             upstream_valid_version,
    #             other_upstream_valid_version
    #         }
    #     }
    # }

    _location = "/_meta.json"

    def __init__(self, saver: "base.BaseSaver"):
        """
        :param saver: the saver the op_store will use to retrieve it's metadata and subsequent ops
        """
        self._saver = saver
        self._all_op_links = self._load_from_saver()  # type: _OpGraph

    def _load_from_saver(self) -> _OpGraph:
        """
        loads and parses all the versions from the meta json
        """
        try:
            mapping = json.loads(self._saver.load(path=self._location).decode("utf-8"))
            return self._parse_mapping(mapping)
        except FileNotFoundError:
            return {}

    def _parse_mapping(
            self, op_version_json: Union[Dict[Text, Dict[Text, Set[Text]]], Dict[Text, Set[Text]]]
    ) -> Union[Dict[Text, Dict[Text, Set[Version]]], Dict[Text, Set[Version]]]:
        """
        parses the saved meta and returns a valid metadata (with versions as object rather than strings)

        :param op_version_json: all the op version metadata in json format
        :return: the op version metadata
        """
        for key, value in op_version_json.items():
            if isinstance(value, dict):
                op_version_json[key] = self._parse_mapping(value)
            if isinstance(value, list):
                op_version_json[key] = {Version.parse(version_str) for version_str in value}
        return op_version_json

    def save(self):
        """
        persists the the op version metadata.
        """
        version_dict_with_str_versions = {
            downstream_op_name: {
                upstream_op_name: [str(version) for version in upstream_data]
                for upstream_op_name, upstream_data in downstream_data.items()
            }
            for downstream_op_name, downstream_data in self._all_op_links.items()
        }
        self._saver.save(json.dumps(version_dict_with_str_versions).encode("utf-8"), path=self._location)

    def get_all_verisons_of_op(self, op: "base.BaseOp", fallback: Any = None) -> Optional[List[Version]]:
        """
        gets all the versions of an op that were previously persisted (the op version and not the upstream one)
        regardless of which pipeline saved it

        :param op: the op to get the previous persisted versions
        :param fallback: the fallback to give back if the op has never been persisted
        """
        all_versions = [
            versions
            for downstream_data in self._all_op_links.values()
            for upstream_op, versions in downstream_data.items()
            if upstream_op == op.name
        ]
        return set.union(*all_versions) if all_versions else None

    def get_validated_links(self, downstream_op_name: Text, upstream_op_name: Text) -> Optional[Set[Version]]:
        return self._all_op_links.get(downstream_op_name, {}).get(upstream_op_name)

    def get_op_bytes_for_version(self, op: "base.BaseOp", version: Version) -> bytes:
        """
        loads the persisted bytes of an op given the version that needs loading

        :param op: the op to laod
        :param version: the version of the op to load
        :return: the bytes of the op
        """
        path = self._build_op_path(op.name, version)
        return self._saver.load(path=path)

    @staticmethod
    def _build_op_path(op_name: str, version: Version) -> str:
        """
        builds the path an op should be persisted at given it's version

        :param op_name: the name of the op to build the path for
        :param version: th the version of that op
        :return: the path at which to save
        """

        return "/models/{}/{}".format(op_name, str(version))

    def save_op_bytes(self, op_to_save: "base.BaseOp", version: Version, op_bytes: bytes):
        """
        saves an op bytes and registers the links that accept this op as upstream (all the ops that use this
        op and are valid to use with this specific op's version

        :param version: the exact version to be used when persisting
        :param op_to_save: the op that needs saving
        :param op_bytes: the bytes of the op to save
        """
        path = self._build_op_path(op_to_save.name, version=version)
        self._saver.save(serialized_object=op_bytes, path=path)

    def register_valid_link(self, downstream_op, upstream_op, upstream_op_version):
        self._all_op_links.setdefault(
            downstream_op if downstream_op is not None else "__end_of_pipe__", {}
        ).setdefault(upstream_op, set()).add(upstream_op_version)