import pandas as pd

import ds_memory

def check_df_equal(a, b):
    eq_rows = (a == b).all(axis='columns')

    if eq_rows.all():
        return True

    else:
        print('Difference')
        print('A:')
        print(a.loc[-eq_rows])
        print('B:')
        print(b.loc[-eq_rows])

        return False


def test_put_success():
    ds = ds_memory.MemoryDataStore()

    df1 = pd.DataFrame({
        'a': range(3)
    })

    ds.put_success('test', df1)

    assert check_df_equal(ds.get_df('test'), df1)

    df2 = pd.DataFrame({
        'a': range(5)
    })

    ds.put_success('test', df2)

    assert check_df_equal(ds.get_df('test'), df2)

    df3 = pd.DataFrame({
        'a': [100, 101, 102]
    })

    # TODO реализовать удаление строк, которых не было в df
    df3_expected = pd.DataFrame({
        'a': [100, 101, 102, 3, 4]
    })

    ds.put_success('test', df3)

    assert check_df_equal(ds.get_df('test'), df3_expected)


def test_put_failure():
    ds = ds_memory.MemoryDataStore()

    df1 = pd.DataFrame({
        'a': range(3)
    })

    ds.put_failure('test', df1.index, 'because')