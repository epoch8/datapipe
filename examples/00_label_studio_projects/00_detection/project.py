import logging
import time

from subprocess import Popen
from functools import partial, update_wrapper
from pathlib import Path
from urllib.parse import urljoin
from datapipe.compute import run_pipeline
from datapipe.label_studio.session import LabelStudioSession

from datapipe.metastore import MetaStore
from datapipe.store.filedir import JSONFile, TableStoreFiledir, PILFile
from datapipe.dsl import BatchTransform, LabelStudioModeration, Catalog, ExternalTable, Table, Pipeline
from datapipe.label_studio.run_server import LabelStudioConfig


def wrapped_partial(func, *args, **kwargs):
    partial_func = partial(func, *args, **kwargs)
    update_wrapper(partial_func, func)
    return partial_func


LABEL_CONFIG = '''<View>
<Text name="text" value="$unique_id"/>
<Image name="image" value="$image"/>
<RectangleLabels name="label" toName="image">
    <Label value="Class1" background="#6600ff"/>
    <Label value="Class2" background="#0000ff"/>
</RectangleLabels>
</View>'''
PROJECT_SETTING = {
    "title": "Detection Project",
    "description": "Detection project",
    "label_config": LABEL_CONFIG,
    "expert_instruction": "",
    "show_instruction": False,
    "show_skip_button": True,
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
}


def convert_to_ls_input_data(
    input_images_df,
    files_url: str
):
    input_images_df['data'] = input_images_df.index.map(
        lambda id: {
            'unique_id': id,
            'image': urljoin(files_url, f"00_dataset/{id}.jpeg")
        }
    )
    return input_images_df[['data']]


def parse_annotations(
    tasks_df
):
    def parse_annotation(annotations):
        bboxes_data = []
        if not annotations:
            return bboxes_data

        annotation = annotations[0]  # maximum_annotations is set as "1" above
        for rectangle in annotation['result']:
            original_height = rectangle['original_height']
            original_width = rectangle['original_width']
            height = rectangle['value']['height']
            width = rectangle['value']['width']
            xmin = rectangle['value']['x']
            ymin = rectangle['value']['y']
            angle = rectangle['value']['rotation']
            label = rectangle['value']['rectanglelabels'][0]
            xmax = xmin + width
            ymax = ymin + height
            xmin = xmin / 100 * original_width
            ymin = ymin / 100 * original_height
            xmax = xmax / 100 * original_width
            ymax = ymax / 100 * original_height
            bboxes_data.append({
                'xmin': int(xmin),
                'ymin': int(ymin),
                'xmax': int(xmax),
                'ymax': int(ymax),
                'angle': angle,
                'label': label
            })
        return bboxes_data

    tasks_df['annotations'] = tasks_df['annotations'].apply(parse_annotation)

    return tasks_df


def run_project(
    data_dir: str,
):
    data_dir = Path(data_dir).absolute()
    (data_dir / 'xx_datatables').mkdir(exist_ok=True)

    catalog = Catalog({
        'input_images': ExternalTable(
            store=TableStoreFiledir(data_dir / '00_dataset' / '{id}.jpeg', PILFile('jpg')),
        ),
        'LS_data_raw': Table(
            store=TableStoreFiledir(data_dir / '01_LS_data_raw' / '{id}.jpeg', JSONFile()),
        ),
        'tasks_raw': Table(  # Updates when someone is annotating
            store=TableStoreFiledir(data_dir / '02_annotations_raw' / '{id}.json', JSONFile()),
        ),
        'tasks_parsed': Table(
            store=TableStoreFiledir(data_dir / '03_annotations' / '{id}.json', JSONFile()),
        )
    })

    label_studio_config = LabelStudioConfig(
        no_browser=True,
        database=data_dir / 'xx_datatables' / 'ls.db',
        internal_host='localhost',
        port='8080',
        username='bobokvsky@epoch8.co',
        password='qwertyisALICE666',
    )
    label_studio_session = LabelStudioSession(
        label_studio_config=label_studio_config
    )
    html_server_host = 'localhost'
    html_server_port = '8081'
    files_url = f'http://{html_server_host}:{html_server_port}/'
    http_server_service = Popen([  # For hosting images files
        'python', '-m', 'http.server', '--bind', html_server_host,
        '-d', str(data_dir), html_server_port,
    ])

    pipeline = Pipeline([
        BatchTransform(
            wrapped_partial(
                convert_to_ls_input_data,
                files_url=files_url,
            ),
            inputs=['input_images'],
            outputs=['LS_data_raw']
        ),
        LabelStudioModeration(
            label_studio_session=label_studio_session,
            project_setting=PROJECT_SETTING,
            inputs=['LS_data_raw'],
            outputs=['tasks_raw'],
        ),
        BatchTransform(
            parse_annotations,
            inputs=['tasks_raw'],
            outputs=['tasks_parsed']
        )
    ])

    ms = MetaStore('sqlite:///' + str(data_dir / 'xx_datatables/metadata.sqlite'))

    def debug_catalog(dt_name):
        df = catalog.get_datatable(ms=ms, name=dt_name).get_data()
        logging.debug(f'{dt_name=}\n{df}\n\n')

    try:
        while True:
            run_pipeline(ms, catalog, pipeline)
            debug_catalog('input_images')
            debug_catalog('LS_data_raw')
            debug_catalog('tasks_raw')
            debug_catalog('tasks_parsed')
            time.sleep(5)

    except KeyboardInterrupt:
        print("\nKeyboard interrupt received, exiting.")
        http_server_service.terminate()
        raise


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(name)s] [%(levelname)s] %(message)s"
    )
    run_project(
        data_dir='data/',
    )
