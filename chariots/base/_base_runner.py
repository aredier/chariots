from abc import ABC, abstractmethod
from typing import Optional, Any

import chariots


class BaseRunner(ABC):
    """
    a runner handles executing a graph of nodes
    """

    @abstractmethod
    def run(self, pipeline: "chariots.Pipeline", pipeline_input: Optional[Any] = None):
        """
        runs a whole pipeline

        :param pipeline_input: the input to be given to the pipeline
        :param pipeline: the pipeline to run

        :return: the output of the graph called on the input if applicable
        """
        pass
