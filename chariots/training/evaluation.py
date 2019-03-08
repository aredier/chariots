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
    """placeholder marker to be attributed to evaluation markers
    """

    def compatible(self, other: "Marker") -> bool:
        return isinstance(other, EvaluationMarker)


class EvaluationMetric(BaseOp):
    """abstract evaluation metric 
    """


    markers = [EvaluationMarker]

    def evaluate(self, other: AbstractOp) -> Report:
        """evaluates a full pipeline (this will run through the pipeline) and aggregates the
        pipeline.
        
        Arguments:
            other {AbstractOp} -- op or pipeline to run through
        
        Returns:
            Report -- the resulting report
        """

        self(other)
        grouped = {}
        for batch_report in self.perform():
            version, report = batch_report[self.markers[0]]
            grouped.setdefault(version, []).append(report)

        return {version: self._aggregate_evaluations(reports)
                for version, reports in grouped.items()}
    
    def _main(self, **kwargs):
        return [str(self.previous_op.version), self._evaluate_batch(**kwargs)]

    @abstractmethod
    def _evaluate_batch(self, **kwargs) -> Mapping[Text, float]:
        """method to evaluate a single batch, this will output a report (in a json file) along with
        any additional inner data (prepended with _) that might be needed to aggregate future 
        batches
        
        Returns:
            Mapping[Text, float] -- the json output file
        """
    
    @abstractmethod
    def _aggregate_evaluations(self, metrics: Iterable[Report]) -> Report:
        """abstract method that aggregates the single reports outputed by `_evaluate_batch`
        
        Arguments:
            metrics {Iterable[Report]} -- the single evaluation json
        
        Returns:
            Report -- the aggregated report
        """



class ClassificationMetrics(EvaluationMetric):

    name = "classification_report"
    
    def __init__(self, y_true: Marker, y_pred: Marker, metrics: Optional[List[Text]] = None):
        self.metrics = metrics or ["accuracy"]
        self.requires = {"y_true": y_true.as_marker(), "y_pred": y_pred.as_marker()}
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

    name = "regression_report"
    
    def __init__(self, y_true: Marker, y_pred: Marker, metrics: Optional[List[Text]] = None):
        self.metrics = metrics or ["mae", "mse"]
        self.requires = {"y_true": y_true.as_marker(), "y_pred": y_pred.as_marker()}

        # TODO be able to use a subset of the metrics
        if metrics != ["mae", "mse"]:
            raise NotImplementedError("regression metrics only account to mae and for now")

    def _evaluate_batch(self, y_true, y_pred) -> Mapping[Text, float]:
        assert len(y_true) == len(y_pred), "inconsistent data"
        _n = len(y_true)
        _absolue_error = sum([abs(true_ind - pred_ind) for true_ind, pred_ind 
                              in zip(y_true, y_pred)])
        _squared_error = sum([(true_ind - pred_ind) ** 2 for true_ind, pred_ind 
                              in zip(y_true, y_pred)])
        return {"_n": _n, "_absolue_error": _absolue_error, "_squared_error": _squared_error,
                "mae": _absolue_error / _n, "mse": _squared_error / _n}
    
    def _aggregate_evaluations(self, metrics: Iterable[Report]) -> Report:
        _n = 0
        _absolue_error = 0
        _squared_error = 0
        for batch_report in metrics:
            _n += batch_report["_n"]
            _absolue_error += batch_report["_absolue_error"]
            _squared_error += batch_report["_squared_error"]
        return {"_n": _n, "_absolue_error": _absolue_error, "_squared_error": _squared_error,
                "mae": _absolue_error / _n, "mse": _squared_error / _n}