from typing import List

import pandas as pd
from googletrans import Translator

from pipe import IncProcess


def translate(df: pd.DataFrame, src: str = 'en', dest: str = 'ru') -> pd.DataFrame:
    translator = Translator()

    for c in df.columns:
        not_null_idx = -df.loc[:,c].isnull()

        translations = translator.translate(list(df.loc[not_null_idx, c]), src=src, dest=dest)
        df.loc[not_null_idx, c] = [i.text for i in translations]
    
    return df


def ProcTranslate(input_cols, src, dest):
    return IncProcess(translate, input_cols=input_cols, kwargs=dict(src=src, dest=dest))