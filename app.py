import pandas as pd
import logging

from pipe import Source, IncProcess, PipeRunner
from ds_memory import MemoryDataStore

from proc_translate import IncTranslate

def lower(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['text'] = df['text'].str.lower()

    return df


class MemSrc(object):
    def __init__(self, data):
        self.data = data
    
    def __call__(self) -> pd.DataFrame:
        return self.data

mem_src = MemSrc(None)


class FailingId(object):
    def __init__(self, should_fail):
        self.should_fail = should_fail
    
    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.should_fail:
            raise Exception('Failed as should')

        return df

failing_id = FailingId(False)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    pd.options.display.float_format = '{:f}'.format

    rnr = PipeRunner(
        MemoryDataStore(),
        [
            ('memsrc',      Source(mem_src)),
            ('failing_id',  IncProcess(failing_id, ['text'])),
            ('lower',       IncProcess(lower, ['text'])),
            ('translate',   IncTranslate(inputs=['text'], src='ru', dest='en')),
        ]
    )

    mem_src.data = pd.DataFrame({
        'text': ['Батон', 'Булка', 'Молоко 0.5л']
    })

    print('*'*10)
    rnr.run()

    mem_src.data = pd.DataFrame({
        'text': ['Батон', 'Булка', 'Молоко 1л', 'Сок добрый']
    })

    print('*'*10)
    rnr.run()

    mem_src.data = pd.DataFrame({
        'text': ['Батон', 'Булка 2', 'Молоко 1л 2', 'Сок добрый 2']
    })

    failing_id.should_fail = True

    print('*'*10)
    rnr.run()
