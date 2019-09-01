from _hashlib import HASH
from typing import Mapping, Text, Union, Any, Dict, List, AnyStr


SymbolicToRealMapping = Mapping[Text, "NodeReference"]
ResultDict = Dict[Union["NodeReference"], Any]
InputNodes = List[Union[AnyStr, "Node"]]
OpStoreMetaJson = Mapping[AnyStr, Mapping[AnyStr, List[Mapping[AnyStr, AnyStr]]]]
OpStoreMeta = Mapping[AnyStr, Mapping[AnyStr, List[Mapping[AnyStr, "Version"]]]]
Hash = Union[HASH, str]
