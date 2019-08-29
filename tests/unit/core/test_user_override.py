import time

import pytest

from chariots.core.nodes import Node
from chariots.core.ops import AbstractOp, OpCallBack
from chariots.core.pipelines import PipelineCallBack, Pipeline, SequentialRunner


class TimerOp(AbstractOp):

    def __init__(self, stop_time):
        self.time = stop_time

    def __call__(self, previous_time = 0):
        time.sleep(self.time)
        return self.time + previous_time


def test_before_op():

    class TimerBefore(TimerOp):
        def __init__(self, stop_time):
            super().__init__(stop_time)
            self.inputs = []

        def before_execution(self, op, args_dict):
            self.inputs.append(args_dict["previous_time"])

    op = TimerBefore(0.01)
    for i in range(10):
        op.execute(i)

    assert op.inputs = list(range(10))



def test_after_op():
    class TimerAfter(TimerOp):
        def __init__(self, op, stop_time):
            super().__init__(stop_time)
            self.inputs = []
            self.outputs = []

        def after_execution(self, op, args_dict, outputs):
            self.inputs.append(args_dict["previous_time"])
            self.outputs.append(outputs)

    op = TimerAfter(0.01)
    for i in range(10):
        op.execute(i)

    assert op.inputs == list(range(10))
    assert op.outputs == list(range(0.01, 10.01, 1))


class OpTimerCallback(OpCallBack):

    def __init__(self):
        self._temp_value = None
        self.timings = []

    def before_execution(self, op, arg_dicts):
        self._temp_value = time.time()

    def after_execution(self, op, arg_dicts, outputs):
        self.timings.append(self._temp_value - time.time())

def test_op_callback():
    cb = OpTimerCallback()
    op = TimerOp(0.01, callbacks=[cb])

    for i in range(100):
        op.execute(0)

        assert 0.99 <= sum(cb.timings) <= 1.01

class TestBeforePipeline(PipelineCallBack):

    def __init__(self):
        self.execution_count = 0

    def before_execution(self, pipeline, args_dict):
        self.execution_count += 1


class TestAfterPipeline(PipelineCallBack):

    def __init__(self):
        self.execution_count = 0

    def after_execution(self, pipeline, args_dict):
        self.execution_count += 1



def test_before_pipeline():

    cb = TestBeforePipeline()
    pipe = Pipeline([Node(TimerOp(0.01), output_nodes="__pipeline_output__")], name="a_pipe", callbacks=[cb])
    runner = SequentialRunner()
    for i in range(10):
        pipe.execute(runner)

    assert cb.execution_count == 10


def test_after_pipeline():
    cb = TestAfterPipeline()
    pipe = Pipeline([Node(TimerOp(0.01), output_nodes="__pipeline_output__")], name="a_pipe", callbacks=[cb])
    runner = SequentialRunner()
    for i in range(10):
        pipe.execute(runner)

    assert cb.execution_count == 10


def test_multiple_callbacks():

    class RaiseOp(AbstractOp):

        def __call__(self, *args, **kwargs):
            raise ValueError

    cb_before = TestBeforePipeline()
    cb_after = TestAfterPipeline()
    pipe = Pipeline([Node(RaiseOp(), output_nodes="__pipeline_output__")], name="a_pipe",
                    callbacks=[cb_after, cb_before])
    runner = SequentialRunner()
    with pytest.raises(ValueError):
        pipe(runner)
    assert cb_before.execution_count == 1
    assert cb_after.execution_count == 0


def test_full_pipeline_call_back():

    class PipeTimerCallBack(PipelineCallBack):

        def __init__(self):
            self.pipe_timer = None
            self.op_timer = None
            self._result_dict = {}

        def before_execution(self, piepline, args_dict):
            self.pipe_timer = time.time()

        def after_execution(self, pipeline, args_dict, output):
            self._result_dict.setdefault(pipeline.name, []).append(time.time() - self.pipe_timer)
            self.pipe_timer = None

        def before_ops_execution(self, op, args_dict):
            self.op_timer = time.time()

        def after_ops_execution(self, op, args_dict, output):
            self._result_dict.setdefault(op.name, []).append(time.time() - self.op_timer)
            self.op_timer = None

        @property
        def result_dict(self):
            return {key: sum(value) / len(value) for key, value in self._result_dict.items()}

    cb = PipeTimerCallBack()
    op = TimerOp(0.01)
    pipe = Pipeline([Node(op, output_nodes="__pipeline_output__")], "my_pipe", [cb])
    runner = SequentialRunner()

    for i in range(50):
        pipe.execute(runner)

    len(cb.result_dict) == 2
    assert "my_pipe" in cb.result_dict
    assert op.name in cb.result_dict
    assert 0.005 <= cb.result_dict[op.name] <= 0.015
    assert 0 <= cb.result_dict["my_pipe"] - cb.result_dict[op.name] <= 0.005
