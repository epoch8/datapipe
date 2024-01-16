from openai import OpenAI, OpenAIError

import pandas as pd
from sqlalchemy import Column, Integer, String

from datapipe.compute import Catalog, DatapipeApp, Pipeline, Table
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransform
from datapipe.step.update_external_table import UpdateExternalTable
from datapipe.store.database import DBConn, TableStoreDB
from datapipe.store.pandas import TableStoreJsonLine

GPT_KEY = "WRITE YOUR OPENAI KEY HERE"

dbconn = DBConn("sqlite:///db.sqlite")
ds = DataStore(dbconn)

dbconn_data = DBConn("sqlite:///data.sqlite")

def process_prompt(
    prompt_df: pd.DataFrame, input_df: pd.DataFrame, 
) -> pd.DataFrame:

    openai_client = OpenAI(api_key=GPT_KEY)

    results = []
    for _, inp in input_df.iterrows():
        for _, prompt in prompt_df.iterrows():

            chat_completion = openai_client.chat.completions.create(
                messages=[
                    {
                        "role": "system",
                        "content": prompt['prompt_text'],
                    },
                    {
                        "role": "user",
                        "content": inp['input_text'],
                    },
                ],
                model="gpt-3.5-turbo",
            )

            answer = chat_completion.choices[0].message.content

            results.append(
                (
                    prompt['prompt_id'],
                    inp['input_id'],
                    prompt['prompt_text'],
                    inp['input_text'],
                    answer
                )
            )

    return pd.DataFrame(
        results,
        columns=[
            "prompt_id",
            "input_id",
            "prompt_text",
            "input_text",
            "answer_text",
        ],
    )     


catalog = Catalog(
    {
        "prompt": Table(
            store=TableStoreDB(
                dbconn=dbconn_data,
                name='prompt',
                data_sql_schema=[
                    Column("prompt_id", Integer, primary_key=True),
                    Column("prompt_text", String),
                ],
                create_table=True,
            )
        ),
        "input": Table(
            store=TableStoreDB(
                dbconn=dbconn_data,
                name='input',
                data_sql_schema=[
                    Column("input_id", Integer, primary_key=True),
                    Column("input_text", String),
                ],
                create_table=True,
            )
        ),
        "output": Table(
            store=TableStoreDB(
                dbconn=dbconn_data,
                name='output',
                data_sql_schema=[
                    Column("prompt_id", Integer, primary_key=True),
                    Column("input_id", Integer, primary_key=True),
                    Column("prompt_text", String),
                    Column("input_text", String),
                    Column("answer_text", String),
                ],
                create_table=True,
            )
        ),
    }
)

pipeline = Pipeline(
    [
        UpdateExternalTable(
            output="prompt",
        ),
        UpdateExternalTable(
            output="input",
        ),
        BatchTransform(
            process_prompt,
            inputs=["prompt", "input"],
            outputs=["output"],
            transform_keys=[
                "prompt_id",
                "input_id",
            ],
            chunk_size=100,
        ),
    ]
)


app = DatapipeApp(ds, catalog, pipeline)