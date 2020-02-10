"""module that tests that all the entry points where users can override default behaviors work properly"""
import time
from typing import Optional, Any, List

import pytest
import numpy as np

from chariots import Pipeline
from chariots.base import BaseOp
from chariots.callbacks import OpCallBack, PipelineCallback
from chariots.nodes import Node
from chariots.runners import SequentialRunner


class TimerOp(BaseOp):
    """op that freezes the pipeline for a certain amount of time and than adds that amount to it's input"""

    def __init__(self, stop_time: float, op_callbacks: Optional[List[OpCallBack]] = None):
        super().__init__(op_callbacks=op_callbacks)
        self.time = stop_time

    def execute(self, previous_time=0):  # pylint: disable=arguments-differ
        time.sleep(self.time)
        return self.time + previous_time


def test_before_op():
    """tests that the before_execution entry-point works"""

    class TimerBefore(TimerOp):
        """inner op"""
        def __init__(self, stop_time):
            super().__init__(stop_time)
            self.inputs = []

        def before_execution(self, args):
            self.inputs.append(args[0])

    timer_op = TimerBefore(0.01)
    for i in range(10):
        timer_op.execute_with_all_callbacks([i])

    assert timer_op.inputs == list(range(10))


def test_after_op():
    """tests that the after_execution entry-point works"""

    class TimerAfter(TimerOp):
        """inner op"""
        def __init__(self, stop_time):
            super().__init__(stop_time)
            self.inputs = []
            self.outputs = []

        def after_execution(self, args, outputs):  # pylint: disable=arguments-differ
            self.inputs.append(args[0])
            self.outputs.append(outputs)

    timer_op = TimerAfter(0.01)
    for i in range(10):
        timer_op.execute_with_all_callbacks([i])

    assert timer_op.inputs == list(range(10))
    assert timer_op.outputs == list(np.arange(0.01, 10.01, 1))


class OpTimerCallback(OpCallBack):
    """callback that times the execution of an op"""

    def __init__(self):
        self._temp_value = None
        self.timings = []

    def before_execution(self, callback_op, args):  # pylint: disable=arguments-differ
        self._temp_value = time.time()

    def after_execution(self, callback_op, args, outputs):  # pylint: disable=arguments-differ, unused-argument
        self.timings.append(time.time() - self._temp_value)
        self._temp_value = None


def test_op_callback():
    """test the basic behavior of a callback"""
    timer_callback = OpTimerCallback()
    timer_op = TimerOp(0.01, op_callbacks=[timer_callback])

    for _ in range(100):
        timer_op.execute_with_all_callbacks([0])

    assert 0.75 <= sum(timer_callback.timings) <= 1.25


class TestBeforePipeline(PipelineCallback):
    """`PipelineCallback` that tests the `before_execution` entry point"""

    def __init__(self):
        self.execution_count = 0

    def before_execution(self, pipeline, args):  # pylint: disable=arguments-differ
        self.execution_count += 1


class TestAfterPipeline(PipelineCallback):
    """`PipelineCallback` that tests the `after_execution` entry point"""

    def __init__(self):
        self.execution_count = 0

    def after_execution(self, pipeline: Pipeline, args: List[Any], output: Any):  # pylint: disable=arguments-differ
        self.execution_count += 1


def test_before_pipeline():
    """tests the `before_execution` entry point of pipeline callbacks"""

    before_callback = TestBeforePipeline()
    pipe = Pipeline([Node(TimerOp(0.01), output_nodes='__pipeline_output__')], name='a_pipe',
                    pipeline_callbacks=[before_callback])
    runner = SequentialRunner()
    for _ in range(10):
        runner.run(pipe)

    assert before_callback.execution_count == 10


def test_after_pipeline():
    """tests the `after_execution` entry point of pipeline callbacks"""
    after_callback = TestAfterPipeline()
    pipe = Pipeline([Node(TimerOp(0.01), output_nodes='__pipeline_output__')], name='a_pipe',
                    pipeline_callbacks=[after_callback])
    runner = SequentialRunner()
    for _ in range(10):
        runner.run(pipe)

    assert after_callback.execution_count == 10


def test_multiple_callbacks():
    """tests having multiple callbacks on one pipeline"""

    class RaiseOp(BaseOp):
        """op that raises when executed"""

        def execute(self, *args, **kwargs):
            raise ValueError

    cb_before = TestBeforePipeline()
    cb_after = TestAfterPipeline()
    pipe = Pipeline([Node(RaiseOp(), output_nodes='__pipeline_output__')], name='a_pipe',
                    pipeline_callbacks=[cb_after, cb_before])
    runner = SequentialRunner()
    with pytest.raises(ValueError):
        runner.run(pipe)
    assert cb_before.execution_count == 1
    assert cb_after.execution_count == 0


def test_full_pipeline_call_back():
    """
    tests a callback that utilizes several all the entry points (`before_execution`, `after_execution`,
    `before_node_execution`, and `after_node_execution`)
     """

    class PipeTimerCallback(PipelineCallback):
        """callback with all entry points used"""

        def __init__(self):
            self.pipe_timer = None
            self.op_timer = None
            self._result_dict = {}

        def before_execution(self, piepline, args):  # pylint: disable=arguments-differ, unused-argument
            self.pipe_timer = time.time()

        def after_execution(self, pipeline, args, output):  # pylint: disable=arguments-differ
            self._result_dict.setdefault(pipeline.name, []).append(time.time() - self.pipe_timer)
            self.pipe_timer = None

        def before_node_execution(self, pipeline: Pipeline, node: 'BaseNode',
                                  args: List[Any]):  # pylint: disable=arguments-differ
            self.op_timer = time.time()

        def after_node_execution(self, pipeline: Pipeline, node: 'BaseNode', args: List[Any],
                                 output):  # pylint: disable=arguments-differ
            self._result_dict.setdefault(node.name, []).append(time.time() - self.op_timer)
            self.op_timer = None

        @property
        def result_dict(self):
            """property with all the results of the entry_points, normalized"""
            return {key: sum(value) / len(value) for key, value in self._result_dict.items()}

    full_timer_callback = PipeTimerCallback()
    my_op = TimerOp(0.01)
    pipe = Pipeline([Node(my_op, output_nodes='__pipeline_output__')], 'my_pipe', [full_timer_callback])
    runner = SequentialRunner()

    for _ in range(50):
        runner.run(pipe)

    assert len(full_timer_callback.result_dict) == 2
    assert 'my_pipe' in full_timer_callback.result_dict
    assert my_op.name in full_timer_callback.result_dict
    assert 0.005 <= full_timer_callback.result_dict[my_op.name] <= 0.015
    assert 0 <= full_timer_callback.result_dict['my_pipe'] - full_timer_callback.result_dict[my_op.name] <= 0.005
