import pandas as pd
import logging

from pipe import Source, IncProcess, PipeRunner, Sample
from ds_memory import MemoryDataStore

import src_gfeed
import proc_translate


def lower(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for c in df.columns:
        df.loc[:, c] = df.loc[:,c].str.lower()

    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    pd.options.display.float_format = '{:f}'.format

    rnr = PipeRunner(
        MemoryDataStore(),
        [
            # ('memsrc',      Source(mem_src)),
            ('src',         src_gfeed.SrcGfeed('file://./ozon_products.xml',)),
            ('sample10',    Sample(n=10)),
            ('lower',       IncProcess(lower, input_cols=['title'])),
            ('translate',   proc_translate.ProcTranslate(input_cols=['title'], src='ru', dest='en')),
        ]
    )

    rnr.run()
