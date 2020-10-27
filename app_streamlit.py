import streamlit as st

import pandas as pd
import logging

from pipe import DataTable, Source, IncProcess, PipeRunner
from proc_translate import IncTranslate

def lower(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['text'] = df['text'].str.lower()

    return df


mem_src_data = {'data': None}

def mem_src() -> pd.DataFrame:
    return mem_src_data['data']


@st.cache
def run_pipeline():
    logging.basicConfig(level=logging.DEBUG)

    pd.options.display.float_format = '{:f}'.format

    rnr = PipeRunner([
        ('memsrc',      Source(mem_src)),
        ('lower',       IncProcess(lower, ['text'])),
        ('translate',   IncTranslate(inputs=['text'], src='ru', dest='en')),
    ])

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

    return rnr.data


data = run_pipeline()

df = pd.concat([i.data for _, i in data], keys=[i for i, _ in data], axis='columns')

st.dataframe(df)
