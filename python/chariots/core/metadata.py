import operator
from typing import List

class Metadata:
    
    def __init__(self):
        self.roots = []
        self.leafs = []
        self._edges = []

    @classmethod
    def merge(cls, metadatas: List["Metadata"]) -> "Metadata":
        pass

    def _merge_single(self, other: "Metadata"):
        self.roots.extend(other.roots)
        self.leafs.extend(other.leafs)
        self._edges.extend(other._edges)

    def __bool__(self):
        return bool(self.roots)

    def chain(self, next_op: "BaseOp", previous_ops = None):
        if not self.roots:
            self.roots.append(next_op)
            self.leafs.append(next_op)
            self._edges.append([None, next_op])
        else:
            previous_ops = previous_ops or self.leafs
            self._edges.append((previous_ops, next_op))
            for previous_op in previous_ops:
                self.leafs.remove(previous_op)
            self.leafs.append(next_op)

    def __repr__(self):
        pretty_edges = [(list(map(operator.attrgetter("signature"), prev)), nxt.signature) if prev is not None else ("og", nxt.signature)
                        for prev, nxt in self._edges]
        return f"<Metadata with graph {pretty_edges}>"

