class VersionError(TypeError):

    @staticmethod
    def handle():
        return "trying to load/execute an outdated version, retrain", 419


class BackendError(ImportError):
    pass
