"""
Callbacks are use to change the default behavior of an op or a pipeline in a reusable way, you can create callbacks to
log performance or timing check output distribution or what ever you need around the pipeline or the ops execution.

There are two main types of callbacks:

- operation callbacks that give ou entry points before and after the execution of this specific op
- pipeline callback that give you entry points before and after the execution of the pipeline and in between each node

the order of execution of the callbacks are as follows:

- pipeline callbacks' `before_execution`
- pipeline callbacks' `before_node_execution` (for each node)
- op callbacks' `before_execution`
- op' `before_execution` method
- op's execute method
- op's `after_execution` method
- op  callbacks' `after_execution`
- pipeline callbacks' `after_node_execution`

During the pipeline's execution, the inputs and outputs of the execution are being provided (when applicable), these are
provided for information, DO NOT TRY TO MODIFY those (this is undefined behavior)
"""
from ._op_callback import OpCallBack
from ._pipeline_callback import PipelineCallback

__all__ = [
    "OpCallBack",
    "PipelineCallback"
]
