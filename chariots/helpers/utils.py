import copy

class SplitPusher:

    def __init__(self, n_pullers: int, iterator=None, shallow: bool = False):
        if shallow:
            raise NotImplementedError
        self._pullers = [SplitPuller(self) for _ in range(n_pullers)]
        self.iterator = iter(iterator) if iterator is not None else None

    @property
    def pullers(self):
        return self._pullers

    def set_iterator(self, iterator):
        self.iterator = iterator

    def __next__(self):
        nxt = next(self.iterator)
        for puller in self._pullers:
            puller.register(copy.deepcopy(nxt))

    def __iter__(self):
        return self

class SplitPuller:

    def __init__(self, pusher: SplitPusher):
        self.pusher = pusher
        self.fifo = []

    def register(self, data):
        self.fifo.append(data)

    def __next__(self):
        if not self.fifo:
            next(self.pusher)
        return self.fifo.pop(0)

    def __iter__(self):
        return self
