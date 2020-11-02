import streamlit as st

import pandas as pd
import logging

from pipe import Source, IncProcess, PipeRunner
from ds_memory import MemoryDataStore
from proc_translate import IncTranslate

def lower(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['text'] = df['text'].str.lower()

    return df


mem_src_data = {'data': None}

def mem_src() -> pd.DataFrame:
    return mem_src_data['data']


ds = MemoryDataStore()
pipeline = [
    ('memsrc',      Source(mem_src)),
    ('lower',       IncProcess(lower, ['text'])),
    ('translate',   IncTranslate(inputs=['text'], src='ru', dest='en')),
]


@st.cache
def run_pipeline(ds, pipeline):
    logging.basicConfig(level=logging.DEBUG)

    pd.options.display.float_format = '{:f}'.format

    rnr = PipeRunner(ds, pipeline)

    mem_src_data['data'] = pd.DataFrame({
        'text': ['Батон', 'Булка', 'Молоко 0.5л']
    })

    rnr.run()

    mem_src_data['data'] = pd.DataFrame({
        'text': ['Батон', 'Булка', 'Молоко 1л', 'Сок добрый']
    })

    rnr.run()

    mem_src_data['data'] = pd.DataFrame({
        'text': ['Батон', 'Булка', 'Молоко 1л', 'Сок добрый', 'Печенье творожное']
    })

    rnr.run()

    return ds


data = run_pipeline(ds, pipeline)

for (name, _) in pipeline:
    st.text(name)
    st.dataframe(data.get_system_df(name))
