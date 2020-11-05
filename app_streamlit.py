import streamlit as st

import pandas as pd
import logging

from pipe import Source, IncProcess, PipeRunner, Sample
from ds_memory import MemoryDataStore
import proc_translate
import src_gfeed


logging.basicConfig(level=logging.DEBUG)
pd.options.display.float_format = '{:f}'.format

def lower(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for c in df.columns:
        df.loc[:, c] = df.loc[:,c].str.lower()

    return df


ds = MemoryDataStore()
pipeline = [
    # ('memsrc',      Source(mem_src)),
    ('src',         src_gfeed.SrcGfeed('file://./ozon_products.xml',)),
    ('sample10',    Sample(n=10)),
    # ('lower',       IncProcess(lower, input_cols=['title'])),
    ('translate',   proc_translate.ProcTranslate(input_cols=['title'], src='en', dest='ru')),
]

rnr = PipeRunner(ds, pipeline)

for (prev_name, cur_name, step) in rnr.pipeline:
    st.text(cur_name)
    rnr.run_step(prev_name, cur_name, step)
    st.dataframe(ds.get_debug_df(cur_name))
