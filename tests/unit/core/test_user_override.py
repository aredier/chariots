import time
from typing import Optional, Any, List

import pytest
import numpy as np

from chariots import Pipeline
from chariots.base import BaseNode, BaseOp
from chariots.callbacks import OpCallBack, PipelineCallback
from chariots.nodes import Node
from chariots.runners import SequentialRunner


class TimerOp(BaseOp):

    def __init__(self, stop_time: float, op_callbacks: Optional[List[OpCallBack]] = None):
        super().__init__(op_callbacks=op_callbacks)
        self.time = stop_time

    def execute(self, previous_time=0):
        time.sleep(self.time)
        return self.time + previous_time


def test_before_op():

    class TimerBefore(TimerOp):
        def __init__(self, stop_time):
            super().__init__(stop_time)
            self.inputs = []

        def before_execution(self, args):
            self.inputs.append(args[0])

    op = TimerBefore(0.01)
    for i in range(10):
        op.execute_with_all_callbacks([i])

    assert op.inputs == list(range(10))


def test_after_op():
    class TimerAfter(TimerOp):
        def __init__(self, stop_time):
            super().__init__(stop_time)
            self.inputs = []
            self.outputs = []

        def after_execution(self, args, outputs):
            self.inputs.append(args[0])
            self.outputs.append(outputs)

    op = TimerAfter(0.01)
    for i in range(10):
        op.execute_with_all_callbacks([i])

    assert op.inputs == list(range(10))
    assert op.outputs == list(np.arange(0.01, 10.01, 1))


class OpTimerCallback(OpCallBack):

    def __init__(self):
        self._temp_value = None
        self.timings = []

    def before_execution(self, op, args):
        self._temp_value = time.time()

    def after_execution(self, op, args, outputs):
        self.timings.append(time.time() - self._temp_value)
        self._temp_value = None


def test_op_callback():
    cb = OpTimerCallback()
    op = TimerOp(0.01, op_callbacks=[cb])

    for i in range(100):
        op.execute_with_all_callbacks([0])

    assert 0.75 <= sum(cb.timings) <= 1.25


class TestBeforePipeline(PipelineCallback):

    def __init__(self):
        self.execution_count = 0

    def before_execution(self, pipeline, args):
        self.execution_count += 1


class TestAfterPipeline(PipelineCallback):

    def __init__(self):
        self.execution_count = 0

    def after_execution(self, pipeline: Pipeline, args: List[Any], output: Any):
        self.execution_count += 1


def test_before_pipeline():

    cb = TestBeforePipeline()
    pipe = Pipeline([Node(TimerOp(0.01), output_nodes="__pipeline_output__")], name="a_pipe", pipeline_callbacks=[cb])
    runner = SequentialRunner()
    for i in range(10):
        runner.run(pipe)

    assert cb.execution_count == 10


def test_after_pipeline():
    cb = TestAfterPipeline()
    pipe = Pipeline([Node(TimerOp(0.01), output_nodes="__pipeline_output__")], name="a_pipe", pipeline_callbacks=[cb])
    runner = SequentialRunner()
    for i in range(10):
        runner.run(pipe)

    assert cb.execution_count == 10


def test_multiple_callbacks():

    class RaiseOp(BaseOp):

        def execute(self, *args, **kwargs):
            raise ValueError

    cb_before = TestBeforePipeline()
    cb_after = TestAfterPipeline()
    pipe = Pipeline([Node(RaiseOp(), output_nodes="__pipeline_output__")], name="a_pipe",
                    pipeline_callbacks=[cb_after, cb_before])
    runner = SequentialRunner()
    with pytest.raises(ValueError):
        runner.run(pipe)
    assert cb_before.execution_count == 1
    assert cb_after.execution_count == 0


def test_full_pipeline_call_back():

    class PipeTimerCallback(PipelineCallback):

        def __init__(self):
            self.pipe_timer = None
            self.op_timer = None
            self._result_dict = {}

        def before_execution(self, piepline, args):
            self.pipe_timer = time.time()

        def after_execution(self, pipeline, args, output):
            self._result_dict.setdefault(pipeline.name, []).append(time.time() - self.pipe_timer)
            self.pipe_timer = None

        def before_node_execution(self, pipeline: Pipeline, node: "BaseNode", args: List[Any]):
            self.op_timer = time.time()

        def after_node_execution(self, pipeline: Pipeline, node: "BaseNode", args: List[Any], output):
            self._result_dict.setdefault(node.name, []).append(time.time() - self.op_timer)
            self.op_timer = None

        @property
        def result_dict(self):
            return {key: sum(value) / len(value) for key, value in self._result_dict.items()}

    cb = PipeTimerCallback()
    my_op = TimerOp(0.01)
    pipe = Pipeline([Node(my_op, output_nodes="__pipeline_output__")], "my_pipe", [cb])
    runner = SequentialRunner()

    for i in range(50):
        runner.run(pipe)

    assert len(cb.result_dict) == 2
    assert "my_pipe" in cb.result_dict
    assert my_op.name in cb.result_dict
    assert 0.005 <= cb.result_dict[my_op.name] <= 0.015
    assert 0 <= cb.result_dict["my_pipe"] - cb.result_dict[my_op.name] <= 0.005
