import copy

class SplitPusher:
    
    def __init__(self, n_pullers: int, iterator, shallow: bool = False):
        if shallow:
            raise NotImplementedError
        self._pullers = [SplitPuller(self) for _ in range(n_pullers)]
        self.iterator = iterator
    
    def __next__(self):
        for puller in self._pullers:
            puller.register(copy.deepcopy(next(self.iterator)))

class SplitPuller:
    
    def __init__(self, pusher: SplitPusher):
        self.pusher = pusher
        self.fifo = []

    def __next__(self):
        if not self.fifo:
            next(self.pusher)
        return self.fifo[0]
    
    def register(self, data):
        self.fifo.append(data)