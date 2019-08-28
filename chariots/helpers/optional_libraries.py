from chariots.helpers.errors import BackendError


def load_pandas():
    try:
        import pandas as pd
        return pd
    except ModuleNotFoundError:
        raise BackendError("pandas backend is not installed")
