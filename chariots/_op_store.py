import json
from typing import Union, Dict, Text, Set, Optional, List

from chariots import base
from chariots.versioning import Version

_OpGraph = Dict[Text, Dict[Text, Set[Version]]]


class OpStore:
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
        persists all the metadata about ops and versions available in the store using the store's saver.

        The saved metadata can be found at `/_meta.json` from the saver's route.
        """
        version_dict_with_str_versions = {
            downstream_op_name: {
                upstream_op_name: [str(version) for version in upstream_data]
                for upstream_op_name, upstream_data in downstream_data.items()
            }
            for downstream_op_name, downstream_data in self._all_op_links.items()
        }
        self._saver.save(json.dumps(version_dict_with_str_versions).encode("utf-8"), path=self._location)

    def get_all_versions_of_op(self, op: "base.BaseOp") -> Optional[List[Version]]:
        """
        returns all the available versions of an op ever persisted in the OpGraph (or any Opgraph using the same
        _meta.json)

        :param op: the op to get the previous persisted versions
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
        loads the persisted bytes of op for a specific version

        :param op: the op that needs to be loaded
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
        saves op_bytes of a specific op to the path /models/<op name>/<version>.

        the version that is used here is the node version (and not the op_version) as nodes might be able to modify
        some behaviors of the versioning of their underlying op

        :param op_to_save: the op that needs to be saved (this will not be saved as is - only the bytes)
        :param version: the exact version to be used when persisting
        :param op_bytes: the bytes of the op to save that will be persisted
        """

        # TODO use the op name rather than the op

        path = self._build_op_path(op_to_save.name, version=version)
        self._saver.save(serialized_object=op_bytes, path=path)

    def register_valid_link(self, downstream_op: Optional[str], upstream_op: "str",
                            upstream_op_version: Version):
        """
        registers a link between an upstream and a downstream op. This means that in future relaods the downstream op
        will whitelist this version for this upstream op

        :param downstream_op: the op that needs to whitelist one of it's inputs' new version
        :param upstream_op: the op that is getting whitelisted as one of the inputs of the downstream op
        :param upstream_op_version: the valid version of the op that is getting whitelisted
        :return:
        """
        self._all_op_links.setdefault(
            downstream_op if downstream_op is not None else "__end_of_pipe__", {}
        ).setdefault(upstream_op, set()).add(upstream_op_version)
