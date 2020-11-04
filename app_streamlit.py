import streamlit as st

import pandas as pd
import logging

from pipe import Source, IncProcess, PipeRunner, Sample
from ds_memory import MemoryDataStore
import proc_translate
import src_gfeed


def lower(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for c in df.columns:
        df.loc[:, c] = df.loc[:,c].str.lower()

    return df


ds = MemoryDataStore()
pipeline = [
    # ('memsrc',      Source(mem_src)),
    ('src',         src_gfeed.SrcGfeed('./ozon_products.xml',)),
    ('sample10',    Sample(n=10)),
    # ('lower',       IncProcess(lower, input_cols=['title'])),
    ('translate',   proc_translate.ProcTranslate(input_cols=['title'], src='en', dest='ru')),
]


@st.cache
def run_pipeline(ds, pipeline):
    logging.basicConfig(level=logging.DEBUG)

    pd.options.display.float_format = '{:f}'.format

    rnr = PipeRunner(ds, pipeline)

    rnr.run()

    return ds


data = run_pipeline(ds, pipeline)

for (name, _) in pipeline:
    st.text(name)
    st.dataframe(data.get_system_df(name))
