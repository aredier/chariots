from typing import Optional

from chariots.core.pipeline import Pipeline
from chariots.core.ops import AbstractOp
from chariots.training import TrainableTrait
from chariots.training.trainable_op import TrainableOp

class TrainablePipeline(TrainableTrait, Pipeline):

    @property
    def fited(self):
        pass

    def fit(self, other: Optional[AbstractOp] = None, mode: Text = "naive"):
        if mode != "naive":
            raise NotImplemented(f"mode {mode} is not implemented")
        remaining_for_training = list(self.all_ops)
        reconnect = other is not None
        if reconnect:
            self(other)
        while remaining_for_training:
            next_op = remaining_for_training.pop()
            if not isinstance(next_op, TrainableOp):
                continue
            if not next_op.fited:
                if next_op.previous_op is not None and next_op.previous_op.ready():
                    next_op.fit()
                    continue
                
                # some requirements are not ready, putting the op backa at the back of the queue
                remaining_for_training.insert(0, next_op)
        if reconnect:
            self.previous_op = None
        