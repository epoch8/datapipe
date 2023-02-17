from datapipe.store.database import DBConn
import time

from subprocess import Popen
from functools import partial, update_wrapper
from pathlib import Path
from urllib.parse import urljoin

from datapipe.compute import build_compute, run_steps
from datapipe.datatable import DataStore
from datapipe.store.filedir import JSONFile, TableStoreFiledir, PILFile
from datapipe.compute import (
    BatchTransform,
    LabelStudioModeration,
    Catalog,
    ExternalTable,
    Table,
    Pipeline,
)


def wrapped_partial(func, *args, **kwargs):
    partial_func = partial(func, *args, **kwargs)
    update_wrapper(partial_func, func)
    return partial_func


LABEL_CONFIG = """<View>
<Image name="image" value="$image"/>
<RectangleLabels name="label" toName="image">
    <Label value="Class1" background="#6600ff"/>
    <Label value="Class2" background="#0000ff"/>
</RectangleLabels>
</View>"""
HOST = "localhost"
LS_PORT = "8080"
HTML_FILES_PORT = "8090"
DATA_DIR = Path("data/").absolute()


def convert_to_ls_input_data(input_images_df, files_url: str):
    input_images_df["image"] = input_images_df.index.map(
        lambda id: urljoin(files_url, f"00_dataset/{id}.jpeg")
    )
    return input_images_df[["image"]]


def parse_annotations(tasks_df):
    def parse_annotation(annotations):
        bboxes_data = []
        if not annotations:
            return bboxes_data

        annotation = annotations[0]  # maximum_annotations is set as "1" above
        for rectangle in annotation["result"]:
            original_height = rectangle["original_height"]
            original_width = rectangle["original_width"]
            height = rectangle["value"]["height"]
            width = rectangle["value"]["width"]
            xmin = rectangle["value"]["x"]
            ymin = rectangle["value"]["y"]
            angle = rectangle["value"]["rotation"]
            label = rectangle["value"]["rectanglelabels"][0]
            xmax = xmin + width
            ymax = ymin + height
            xmin = xmin / 100 * original_width
            ymin = ymin / 100 * original_height
            xmax = xmax / 100 * original_width
            ymax = ymax / 100 * original_height
            bboxes_data.append(
                {
                    "xmin": int(xmin),
                    "ymin": int(ymin),
                    "xmax": int(xmax),
                    "ymax": int(ymax),
                    "angle": angle,
                    "label": label,
                }
            )
        return bboxes_data

    tasks_df["annotations"] = tasks_df["annotations"].apply(parse_annotation)

    return tasks_df


(DATA_DIR / "xx_datatables").mkdir(exist_ok=True)

ds = DataStore(
    DBConn("sqlite+pysqlite3:///" + str(DATA_DIR / "xx_datatables/metadata.sqlite"))
)
catalog = Catalog(
    {
        "00_input_images": ExternalTable(
            store=TableStoreFiledir(
                DATA_DIR / "00_dataset" / "{id}.jpeg", PILFile("jpg")
            ),
        ),
        "01_LS_data_raw": Table(
            store=TableStoreFiledir(
                DATA_DIR / "01_LS_data_raw" / "{id}.json", JSONFile()
            ),
        ),
        "02_annotations_raw": Table(  # Updates when someone is annotating
            store=TableStoreFiledir(
                DATA_DIR / "02_annotations_raw" / "{id}.json", JSONFile()
            ),
        ),
        "03_annotations": Table(
            store=TableStoreFiledir(
                DATA_DIR / "03_annotations" / "{id}.json", JSONFile()
            ),
        ),
    }
)

pipeline = Pipeline(
    [
        BatchTransform(
            wrapped_partial(
                convert_to_ls_input_data,
                files_url=f"http://{HOST}:{HTML_FILES_PORT}/",
            ),
            inputs=["00_input_images"],
            outputs=["01_LS_data_raw"],
        ),
        LabelStudioModeration(
            ls_url=f"http://{HOST}:{LS_PORT}/",
            inputs=["01_LS_data_raw"],
            outputs=["02_annotations_raw"],
            auth=("moderation@epoch8.co", "qwerty123"),
            project_title="Detection Project",
            project_description="Detection Project!",
            project_label_config=LABEL_CONFIG,
            data=["image"],
        ),
        BatchTransform(
            parse_annotations, inputs=["02_annotations_raw"], outputs=["03_annotations"]
        ),
    ]
)

steps = build_compute(ds, catalog, pipeline)


if __name__ == "__main__":
    label_studio_service = Popen(
        [
            "label-studio",
            "--database",
            str(DATA_DIR / "xx_datatables" / "ls.db"),
            "--internal-host",
            HOST,
            "--port",
            LS_PORT,
            "--no-browser",
        ]
    )

    http_server_service = Popen(
        [  # For hosting images files
            "python",
            "-m",
            "http.server",
            "--bind",
            HOST,
            "-d",
            str(DATA_DIR),
            HTML_FILES_PORT,
        ]
    )

    try:
        while True:
            run_steps(ds, steps)
            time.sleep(5)
    finally:
        label_studio_service.terminate()
        http_server_service.terminate()
