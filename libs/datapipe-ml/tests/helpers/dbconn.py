import sys
from pathlib import Path


def get_sqlite_dbconnstr(path: Path | None = None) -> str:
    db_path = ":memory:" if path is None else str(path)
    if sys.platform == "darwin":
        return f"sqlite:///{db_path}"
    return f"sqlite+pysqlite3:///{db_path}"
