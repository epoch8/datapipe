import pandas as pd
from googletrans import Translator

from pipe import IncProcess


def translate_columns(translator: Translator, src: str, dest: str, df: pd.DataFrame) -> pd.DataFrame:
    for c in df.columns:
        translations = translator.translate(list(df[c]), src=src, dest=dest)
        df[c] = [i.text for i in translations]
    
    return df


class IncTranslate(IncProcess):
    def __init__(self, inputs, src='en', dest='ru'):
        self.translator = Translator()

        IncProcess.__init__(
            self, 
            func=lambda df: translate_columns(self.translator, src=src, dest=dest, df=df),
            inputs=inputs
        )
