from typing import Mapping, Text, Union, Any, Dict, List

from chariots.core import versioning

SymbolicToRealMapping = Mapping[Text, Union["Node", "ReservedNodes"]]
ResultDict = Dict[Union["Node", "ReservedNodes"], Any]
InputNodes = List[Union[str, "Node"]]
OpStoreMetaJson = Mapping[str, Mapping[str, List[Mapping[str, str]]]]
OpStoreMeta = Mapping[str, Mapping[str, List[Mapping[str, versioning.Version]]]]
