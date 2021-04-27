import time

from subprocess import Popen
from functools import partial, update_wrapper
from pathlib import Path
from urllib.parse import urljoin

from datapipe.compute import build_compute, run_steps
from datapipe.metastore import MetaStore
from datapipe.store.filedir import JSONFile, TableStoreFiledir, PILFile
from datapipe.dsl import BatchTransform, LabelStudioModeration, Catalog, ExternalTable, Table, Pipeline


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
HOST = 'localhost'
LS_PORT = '8080'
HTML_FILES_PORT = '8090'


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

    ms = MetaStore('sqlite:///' + str(data_dir / 'xx_datatables/metadata.sqlite'))
    catalog = Catalog({
        'input_images': ExternalTable(
            store=TableStoreFiledir(data_dir / '00_dataset' / '{id}.jpeg', PILFile('jpg')),
        ),
        'LS_data_raw': Table(
            store=TableStoreFiledir(data_dir / '01_LS_data_raw' / '{id}.json', JSONFile()),
        ),
        'annotation_raw': Table(  # Updates when someone is annotating
            store=TableStoreFiledir(data_dir / '02_annotations_raw' / '{id}.json', JSONFile()),
        ),
        'annotation_parsed': Table(
            store=TableStoreFiledir(data_dir / '03_annotations' / '{id}.json', JSONFile()),
        )
    })

    pipeline = Pipeline([
        BatchTransform(
            wrapped_partial(
                convert_to_ls_input_data,
                files_url=f'http://{HOST}:{HTML_FILES_PORT}/',
            ),
            inputs=['input_images'],
            outputs=['LS_data_raw']
        ),
        LabelStudioModeration(
            ls_url=f'http://{HOST}:{LS_PORT}/',
            project_setting=PROJECT_SETTING,
            inputs=['LS_data_raw'],
            outputs=['annotation_raw'],
        ),
        BatchTransform(
            parse_annotations,
            inputs=['annotation_raw'],
            outputs=['annotation_parsed']
        )
    ])

    steps = build_compute(ms, catalog, pipeline)

    try:
        while True:
            run_steps(ms, steps)
            time.sleep(5)

    except KeyboardInterrupt:
        print("\nKeyboard interrupt received, exiting.")
        raise


if __name__ == '__main__':
    data_dir = Path('data/')
    (data_dir / 'xx_datatables').mkdir(exist_ok=True)
    label_studio_service = Popen([
        'label-studio',
        '--database', str(data_dir / 'xx_datatables' / 'ls.db'),
        '--internal-host', HOST,
        '--port', LS_PORT,
        '--no-browser'
    ])

    http_server_service = Popen([  # For hosting images files
        'python', '-m', 'http.server',
        '--bind', HOST,
        '-d', str(data_dir),
        HTML_FILES_PORT
    ])

    try:
        run_project(
            data_dir='data/',
        )
    except Exception:
        label_studio_service.terminate()
        http_server_service.terminate()
        raise
