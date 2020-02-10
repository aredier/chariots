"""module with all the custom errors of Chariots"""


class VersionError(TypeError):
    """error when their is a non-validated version trying to be executed"""

    @staticmethod
    def handle():
        """handles the error to return the proper error message through HTTP"""
        return 'trying to load/execute an outdated version, retrain', 419


class BackendError(ImportError):
    """error to be raised in the client when their is a pipeline execution fail"""
