from typing import Mapping, Text, Union, Any, Dict, List

SymbolicToRealMapping = Mapping[Text, Union["Node", "ReservedNodes"]]
ResultDict = Dict[Union["Node", "ReservedNodes"], Any]
InputNodes = List[Union[str, "Node"]]
