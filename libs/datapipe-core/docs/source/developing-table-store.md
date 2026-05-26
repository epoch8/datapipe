# Developing TableStore

## When you need it?

If you need Datapipe to read or write data to a specific database which is not
supported out of the box, you will have to write custom TableStore
implementation.

## TableStore functionality overview

TBD

## Testing

For testing standard TableStore implementation functionality there's a base set
of tests, implemented in `datapipe.store.tests.abstract.AbstractBaseStoreTests`.

This is a `pytest` compatible test class. In order to use this set of tests you
need to: 

1. Create `TestYourStore` class in tests of your module which inherits from
`AbstractBaseStoreTests`
1. Implement `store_maker` fixture which returns a function that creates your
   table store given a specific schema

Example:

```
import pytest

from datapipe.store.redis import RedisStore
from datapipe.store.tests.abstract import AbstractBaseStoreTests
from datapipe.types import DataSchema


class TestRedisStore(AbstractBaseStoreTests):
    @pytest.fixture
    def store_maker(self):
        def make_redis_store(data_schema: DataSchema):
            return RedisStore(
                connection="redis://localhost",
                name="test",
                data_sql_schema=data_schema,
            )

        return make_redis_store
```

This will instantiate a suite of common tests for your store.