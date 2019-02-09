
from typing import List

class Metadata:
    
    def __init__(self):
        self.roots = set()
        self.leafs = set()
        self._edges = {}

    @classmethod
    def merge(cls, metadatas: List["Metadata"]) -> "Metadata":
        NotImplemented

    def chain(self, next_op: "BaseOp", node = None):
        if len(self.leafs) > 1 and node is None:
            raise ValueError("cannot leave leaf unspecified when the metadata has multiple leaves")
        if not self.roots:
            self.roots.add(next_op)
            self.leafs.add(next_op)
        else:
            node = node or self.leafs.pop()
            self._edges.setdefault(node, []).append(next_op)
            self.leafs.discard(node)
            self.leafs.add(next_op)
