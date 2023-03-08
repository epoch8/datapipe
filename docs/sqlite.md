# Using with SQLite

Python comes with some ([at least
3.7.15](https://docs.python.org/3/library/sqlite3.html)) version of SQLite
included.

Unfortunately for `datapipe` we need at least 3.39.0 version due to usage of
`FULL OUTER JOIN` in some queries. That's why we can't rely on Python embedded
sqlite module.

## Installation

We configured `sqlite` extra in `datapipe-core` package, which installs
`pysqlite3-binary` and `sqlalchemy-pysqlite3`. With versions selected we can
guarantee that installed sqlite3 version is sufficient.

So specifying `datapipe-core` dependency with `sqlite` extra will provide
correct dependencies.

```toml
# pyproject.toml
datapipe-core = {version="^0.11.11", extras=["sqlite"]}
```

## Gotchas

Alongside with `pysqlite3-binary` there's a package `pysqlite3`. In our
experience `pysqlite3` package sometimes comes with old version of sqlite3,
please be aware.

## Usage

In order to use sqlite3 as a storage for metadata you should specify dbconn with
`"sqlite+pysqlite3://"` driver:

```python
dbconn = DBConn("sqlite+pysqlite3:///db.sqlite")
```
