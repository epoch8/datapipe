# %%
from datapipe.label_studio.session import LabelStudioSession

# %%
sess = LabelStudioSession(
    ls_url='http://localhost:8080/',
    auth=('moderation@epoch8.co', 'qwerty123'),
)

# %%
sess.is_service_up()

# %%
#sess.sign_up()
sess.login()
# %%
sess.is_auth_ok()
# %%
project = sess.create_project({
    "title": 'test',
    "description": 'test',
    "label_config":  """
    <View>
        <Text name="intro" value="Верна ли следующая категория для этого текста? Если нет - укажите новую" />
        <Text name="predictedCategory" value="$prediction" />
        <Text name="text" value="$text" />
        <Choices name="category" toName="text" choice="single-radio">
            <Choice value="Другое" />
            <Choice value="Пособия" />
            <Choice value="Есиа" />
            <Choice value="Выплата 3-7 лет" />
            <Choice value="Запись" />
            <Choice value="Вакцинация" />
            <Choice value="Справка" />
            <Choice value="Сертификат" />
            <Choice value="Пенсионный" />
            <Choice value="Ребенок" />
            <Choice value="Оплата" />
            <Choice value="Налог" />
            <Choice value="Голосование (ПОС)" />
            <Choice value="Выписка" />
            <Choice value="Маткап" />
            <Choice value="Решаем вместе (жалоба)" />
            <Choice value="Паспорт" />
            <Choice value="Электронный документ" />
            <Choice value="Возврат" />
            <Choice value="Загран-паспорт" />
        </Choices>
    </View>
""",
    "expert_instruction": "",
    "show_instruction": False,
    "show_skip_button": False,
    "enable_empty_annotation": True,
    "show_annotation_history": False,
    "organization": 1,
    "color": "#FFFFFF",
    "maximum_annotations": 1,
    "is_published": False,
    "model_version": "",
    "is_draft": False,
    "min_annotations_to_start_training": 10,
    "show_collab_predictions": True,
    "sampling": "Sequential sampling",
    "show_ground_truth_first": True,
    "show_overlap_first": True,
    "overlap_cohort_percentage": 100,
    "task_data_login": None,
    "task_data_password": None,
    "control_weights": {}
})

# %%
sess.upload_tasks(
    [
        {"text": "aaa", "prediction": "aaa"}
    ],
    1
)
# %%
sess.upload_task(
    {"text": "aaa", "prediction": "aaa"},
    project_id=1
)
