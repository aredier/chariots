from operator import attrgetter
from typing import List
from typing import Set
from typing import Text
from typing import Any
from typing import Optional

from chariots.core.ops import AbstractOp
from chariots.core.saving import Savable, Saver
from chariots.core.dataset import DataSet
from chariots.training import TrainableTrait


class Pipeline(TrainableTrait, AbstractOp):
    """
    a pipeline represents a graph of operations and provides helper methods to perform on all 
    relevant ops of the graph (saving, training, ...)
    a pipeline is an op itself and can be used where a normal trainable op would have (even inside
    another pipeline)
    """

    def __init__(self, input_op: Optional[AbstractOp] = None,
                 output_op: Optional[AbstractOp] = None):
        """
        both inputop and output op have to be set or non of them. if you want to create a piepline 
        from one op and build on, use the add method on an empty pipeline

        Keyword Arguments:
            input_op  -- the input of the graph this pipeline represents (default: {None})
            output_op -- the output of the graph this pipeline represents (default: {None})

        Raises:
            ValueError -- if only one of input and output op is set 
        """

        self._op_graph = []
        if (input_op is None) != (output_op is None):
            raise ValueError("input_op and output op have either to be both set or None of them has"\
                             " to be set")
        self.input_op = input_op
        self.output_op = output_op
        if self.input_op is not None:
            self._build_op_graph(self.input_op, self.output_op)

    @property
    def ready(self):
        return self.output_op.ready

    @property
    def previous_op(self):
        return self.input_op.previous_op

    @property
    def all_ops(self) -> List[AbstractOp]:
        """all the ops in the pipeline (order is deterministic but not guarenteed to be from input
        ro output)

        Returns:
            list with all the ops
        """

        return [*[upstream for upstream, _ in self._op_graph], self.output_op]

    @property
    def fited(self):
        """
        whether or not the pipeline is fited
        """
        return self.output_op.ready


    def _build_op_graph(self, input_op: AbstractOp, output_op: AbstractOp):
        """
        builds the internal op graph of the pipeline from an input and an output pipeline

        Arguments:
            input_op -- the input of the graph to build
            output_op -- the output of the graph

        Raises:
            ValueError -- if the graph makes reference to ops beyond input op or if input op is not
                connected to output op
        """

        reading_heads = [output_op]
        while len(reading_heads):
            op_of_interest = reading_heads.pop()
            if op_of_interest == input_op:
                continue
            if op_of_interest.previous_op is None:
                raise ValueError(f"{op_of_interest.name} doesn't seem to converge to provided input ops")
            if isinstance(op_of_interest.previous_op, list):
                next_reading_heads = op_of_interest.previous_op
            else:
                next_reading_heads =[op_of_interest.previous_op]
            for next_head in next_reading_heads:
                self._op_graph.append((next_head, op_of_interest))
                reading_heads.append(next_head)
                self._link_versions(next_head)

    def perform(self) -> DataSet:
        return self.output_op.perform()

    def __call__(self, other: AbstractOp) -> AbstractOp:
        if self.input_op is not None:
            self.input_op(other)
        return self

    def add(self, other: AbstractOp, head=None):
        """
        adds another op to the pipeline after output op
        """
        self._link_versions(other)
        if self.input_op is None:
            self.input_op = other
            self.output_op = other
        else:
            self._op_graph.append((self.output_op, other))
            self.output_op = other(self.output_op)
    def fit(self, other: Optional[AbstractOp] = None, mode: Text = "naive"):
        """
        fiting the pipeline
            :param other:Optional[AbstractOp]=None: optional pipeline or op on which to fit the pipeline
            :param mode:Text="naive": NotImplemented the mode of training (repartition of the training
            data in sequential training)
        """
        called_with_op = other is not None
        if called_with_op:
            self(other)
        if mode != "naive":
            raise NotImplementedError(f"mode {mode} is not implemented")
        next_ops = [self.input_op]
        while len(next_ops):
            op_of_interest = next_ops.pop(0)
            if not isinstance(op_of_interest, TrainableTrait):
                next_ops.extend([downstream for upstream, downstream in self._op_graph if upstream == op_of_interest])
                continue
            if not op_of_interest.previous_op.ready:
                next_ops.append(op_of_interest)
                continue
            op_of_interest.fit()
            next_ops.extend([downstream for upstream, downstream in self._op_graph if upstream == op_of_interest])
        if called_with_op:
            self.input_op.previous_op = None

    def save(self, saver: Saver):
        """saves all the ops inside the pipeline

        Arguments:
            saver -- the saver to handle the saving
        """

        for op in self.all_ops:
            if isinstance(op, Savable):
                op.save(saver)

    def load(self, saver: Saver):
        """loads the ops in the graph that have been persisted

        Arguments:
            saver -- the saver to handel io
        """

        unloaded_ops = self.all_ops
        while len(unloaded_ops):
            op_of_interest = unloaded_ops.pop(0)
            if not isinstance(op_of_interest, Savable):
                continue
            if op_of_interest.previous_op is not None and op_of_interest.previous_op in unloaded_ops:
                unloaded_ops.append(op_of_interest)
                continue
            loaded_op = op_of_interest.load(saver)
            print("fited at loading", loaded_op.fited)
            self._replace_op_in_graph(op_of_interest, loaded_op)

    def _replace_op_in_graph(self, op_to_replace: AbstractOp, replacing_op: AbstractOp):
        """replaces an op in the graph with another"""

        res_op_graph = []
        for downstream, upstream in self._op_graph:
            downstream = replacing_op if downstream == op_to_replace else downstream
            upstream = replacing_op if upstream == op_to_replace else upstream
            upstream(downstream)
            res_op_graph.append((upstream, downstream))
        self._op_graph = res_op_graph

        if op_to_replace == self.input_op:
            self.input_op = replacing_op
        if op_to_replace == self.output_op:
            self.output_op = replacing_op