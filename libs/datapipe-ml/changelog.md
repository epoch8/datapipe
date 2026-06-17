# 0.5.0
* Огромный рефакторинг -- теперь все модели обучаются по общей схеме, контролируемой оркестратором (`datapipe_ml.training.orchestrator`)
* `CountMetrics_FrozenDataset_DetectionModel` и `CountMetrics_FrozenDataset_ClassificationModel` были выпилены.
* Посчет метрики отрефакторен -- теперь метрики считаются сначала на каждой картинке отдельно, затем считается за раз одним sql-запросом. Это приведет к сокращению времени работы на посчет метрик, особенно, когда данных и моделей много.
* Добавлены примеры обучения в `examples`! Их можно так же использовать в качестве **тестов**.

# 0.4.0
* Все три класса заморозки были объединены общим классом `FreezeDataset` с общим кодом (finally!)
* `FreezeDataset` теперь использует совершенную логику через `select sql`, что означает стократное ускорение. Заморозки перестают висеть с увеличением выборки и замороженных датасетов. При миграции удалите старые таблицы трансформаций (новых не будет -- это ок, там теперь `DatatableTransform` вместо дефолтного `BatchTransform`).
* (Для yolov5 моделей): Иногда импортнутые библиотеки мешают загружать модельку через torch.hub.load, для этого добавил аргумент `modules_to_hide_when_loading_detection_model`, который временно скрывает модули при загрузке модели
* У `Inference_And_FindBestThresholdsPerClasssOnSubset_DetectionModel` добавлен аргумент `minimum_detection_score=0.10`, который будет использовать детектор при инференсе (было: `0.01` всегда). 
* У обучения добавился новый аргумент `model_suffix='_default'` -- суффикс папки модели, чтобы можно было отличить разные модели друг от друга.


# 0.3.0
* Добавлено обучение так детекции, так и сегменатции на основе YoloV8 (спасибо @Varfalamei)
* В инференсе детекции и сегментации добавлен аргумент `prediction_threshold` на случай, если нужно игнорировать лучший трешхолд и посчитать инференс с переопределенным `prediction_threshold`.
* Добавлена новая колонка `calc__images_support` в таблицах метрик.
* Добавлены `Inference_And_FindBestThresholdsPerClasssOnSubset_DetectionModel` и `Inference_UsingThresholdsPerClasss_DetectionModel` для типовых задач, когда нужно по каждому классу выделять свои пороги для скоров

# 0.2.1
* Исправлен баг с заморозкой датасетов, когда старый замороженный датасет удалялся при новой заморозке
* Исправлен баг в `Train_Tensorflow_ClassificationModel`, когда аргумент `class_weight` не работал из-за того, что данные из `tf.Dataset` имели неизвестный `shape`.
* Исправлен баг на проверку наличии в таблице уже обученных моделей, если название id колонки модельки указано иное

# 0.2.0
* Нововведение: теперь можно обучать иерархические детекторы и классификаторы, когда нужно обучать конкретную модельку в зависимости от иерархии исходных данных. Например, если у вас есть N стран, и в каждой стране есть свой список классов, то можно обучать сразу N разных классификаторов, по одному с каждой страны.
  * Для этого достаточно вписать в соответствующих шагах аргументы `detection_frozen_dataset_primary_keys` или `detection_model_primary_keys`, `classification_frozen_dataset_primary_keys` или `classification_model_primary_keys` дополнительные колонки, по которым происходит иерархия. Например, `detection_frozen_dataset_primary_keys=["country", "detection_frozen_dataset_id"]` и `detection_model_primary_keys=["country", "detection_model_id"]`.
За подробностями и консультацией обращайтесь к @bobokvsky

* Теперь primary колонки в заморозках (`detection_frozen_dataset_id`, `classification_frozen_dataset_id`) и в моделях (`detection_model_id` и `classification_model_id`) можно переименовать: в соответствующих шагах были добавлены аргументы `detection_frozen_dataset_id__name`, `classification_frozen_dataset_id__name`, `detection_model_id__name` и `classification_model_id__name`.

* Заморозка датасетов (`DetectionFreezeDataset` и `ClassificationFreezeDataset`):
  * Теперь в качестве `input__image` может принимать таблицу вида `TableStoreFiledir`, нужно вписать `image__image_path__name="filepath"`
  * Ослаблено требование на primary ключи у входной таблицы `input__subset__has__image`: теперь достаточно, чтобы он имел хотя бы один primary ключ из списка `primary_keys`. 

* Инференс:
  * Теперь в качестве `input__image` может принимать таблицу вида `TableStoreFiledir`, нужно вписать `image__image_path__name="filepath"`
* Посчет метрик:
  * Ослаблено требование на primary ключи у входной таблицы `input__subset__has__image`: теперь достаточно, чтобы он имел хотя бы один primary ключ из списка `primary_keys`. 

* Шаг `Train_YoloV5_DetectionModel`: 
  * Починен баг падения, когда в обучении участвуют облачные пути
  * убран аргумент `class_names`, теперь он вычисляется динамически исходя из замороженного датасета, и как следствие добавился новый обязательный аргумент: `output__detection_frozen_dataset__class_names` -- название выходной таблички с описанием списка классов. 
  * Аргумент `output__detection_frozen_dataset__coco_txt` был назван неправильно, он был заменен на аргумент `output__detection_frozen_dataset__yolo_txt`
  * Миграция на новую версию не должна быть страшной: в функции обучении стоит проверка, обучилась ли моделька на данном замороженном датасете.

* Шаг `Train_Tensorflow_ClassificationModel`:
  * `TF_ClassificationTrainingConfig`: добавлен аргумент `class_weight: bool`, который включает нормировку весов для несбалансированных классов.
  * Добавлен аргумент `clean_checkpoints_after_train: bool`, который очищает все чекпоинты кроме последних двух после конца обучения модели. По дефолту теперь `False`.
