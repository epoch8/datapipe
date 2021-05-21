import numpy as np
import pandas as pd
from datapipe.texts import get_embedder_conversion, get_classifier_conversion


def test_classifier_conversion():
    texts = ['пастила Хрустящие кус без сах 35г Белев',
             'Скраб д/тела ECOLAB солевой Лифтинг',
             'БАРС ошейник инсектоакорицид. д/собак мелк.пород',
             'Тарелки глубокие для супа',
             'Ноутбук Lenovo',
             'Компьютерная мышь logitech']
    conversion = get_embedder_conversion('http://c12n-common-embedder-v6.research.svc.cluster.local/v1/models/c12n-common-embedder-v6:predict',
                                         10,
                                         10)
    response = conversion(pd.DataFrame({"text": texts}))
    assert np.all(
        np.abs(
            np.array(response["embedding"].tolist())[:, 0] -
            np.array([-0.34, 0.49, 0.03, 0.21, 0.13, -0.04])
        ) < 0.1
    )
