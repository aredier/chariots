from typing import Mapping, Text, Union, Any, Dict

SymbolicToRealMapping = Mapping[Text, Union["Node", "ReservedNodes"]]
ResultDict = Dict[Union["Node", "ReservedNodes"], Any]
