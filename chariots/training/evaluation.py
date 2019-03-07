from abc import abstractmethod
from typing import Mapping
from typing  import Text
from typing import Iterable
from typing import List
from typing import Optional

from chariots.core.markers import Marker
from chariots.core.ops import AbstractOp
from chariots.core.ops import BaseOp
from chariots.core.versioning import Version

Report = Mapping[Text, Mapping[Text, float]]

class EvaluationMarker(Marker):

    def compatible(self, other: "Marker") -> bool:
        return isinstance(other, EvaluationMarker)


class EvaluationMetric(BaseOp):

    markers = [EvaluationMarker]

    def evaluate(self, other: AbstractOp) -> Report:
        self(other)
        grouped = {}
        for batch_report in self.perform():
            version, report = batch_report[self.markers[0]]
            grouped.setdefault(version, []).append(report)

        return {version: self._aggregate_evaluations(reports) for version, reprorts in grouped}
    
    def _main(self, **kwargs):
        return (str(self.previous_op.version), self._evaluate_batch(self, **kwargs))

    @abstractmethod
    def _evaluate_batch(self, **kwargs) -> Mapping[Text, float]:
        pass
    
    @abstractmethod
    def _aggregate_evaluations(self, metrics: Iterable[Report]) -> Report:
        pass


class ClassificationMetrics(EvaluationMetric):

    name = "classification report"
    
    def __init__(self, metrics: Optional[List[Text]] = None):
        self.metrics = metrics or ["accuracy"]
        if metrics != ["accuracy"]:
            raise NotImplementedError("classification metrics only account to accuracy for now")

    def _evaluate_batch(self, y_true, y_pred) -> Mapping[Text, float]:
        assert len(y_true) == len(y_pred), "inconsistent data"
        _n = len(y_true)
        _correct = sum(true_ind == pred_ind for true_ind, pred_ind in zip(y_true, y_pred))
        return {"_n": _n, "_correct": _correct, "accuracy": _correct / _n}
    
    def _aggregate_evaluations(self, metrics: Iterable[Report]) -> Report:
        _n = 0
        _correct = 0
        for batch_report in metrics:
            _n += batch_report["_n"]
            _correct += batch_report["_correct"]
        return {"_n": _n, "_correct": _correct, "accuracy": _correct / _n}
        

class RegresionMetrics(EvaluationMetric):
    pass