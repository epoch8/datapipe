def sqlite_connstr(path: str = "db.sqlite") -> str:
    # pysqlite3 (datapipe-core[sqlite]) ships SQLite >= 3.39 with FULL OUTER JOIN support.
    return f"sqlite+pysqlite3:///{path}"

