import logging
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Literal, Tuple, Union
from multiprocessing import Process

from c12n_pipe.io.data_catalog import DataCatalog
from c12n_pipe.datatable import gen_process_many, inc_process_many
from c12n_pipe.label_studio_utils.label_studio_c12n import LabelStudioConfig, run_app
from label_studio.project import Project
import pandas as pd


logger = logging.getLogger('c12n_pipe.node')


class StoreNode:
    def __init__(
        self,
        proc_func: Callable[[], Iterator[Tuple[pd.DataFrame, ...]]],
        outputs: List[str],
        kwargs: Dict[str, Any] = {},
    ):
        self.proc_func = proc_func
        self.kwargs = kwargs
        self.outputs = outputs

    def run(self, data_catalog: DataCatalog, chunksize: int = 1000):
        outputs_dt = [data_catalog.get_data_table(output) for output in self.outputs]
        gen_process_many(
            dts=outputs_dt,
            proc_func=self.proc_func,
            **self.kwargs
        )

    @property
    def name(self):
        return self.proc_func.__name__


class PythonNode:
    def __init__(
        self,
        proc_func: Callable,
        inputs: List[str],
        outputs: List[str],
        kwargs: Dict[str, Any] = {}
    ):
        self.proc_func = proc_func
        self.inputs = inputs
        self.outputs = outputs
        self.kwargs = kwargs

    def run(
        self,
        data_catalog: DataCatalog,
        chunksize: int = 1000
    ):
        inputs_dt = [data_catalog.get_data_table(input) for input in self.inputs]
        outputs_dt = [data_catalog.get_data_table(output) for output in self.outputs]
        inc_process_many(
            ds=data_catalog.data_store,
            input_dts=inputs_dt,
            res_dts=outputs_dt,
            proc_func=self.proc_func,
            chunksize=chunksize,
            **self.kwargs
        )

    @property
    def name(self):
        return self.proc_func.__name__


class LabelStudioNode:
    def __init__(
        self,
        project_path: Union[str, Path],
        input: str,
        output: str,
        port: int,
        label_config: str = None,
        log_level: Literal['DEBUG', 'INFO', 'WARNING', 'ERROR'] = 'INFO'
    ):
        self.process = None
        self.project_path = Path(project_path)
        self.input = input
        self.output = output
        self.project_config = LabelStudioConfig(
            project_name=str(self.project_path),
            source='tasks-json-modified',
            source_path='tasks.json',
            source_params={
                'data_table_name': input,
            },
            target='completions-dir-modified',
            target_path='completions',
            target_params={
                'data_table_name': output,
            },
            port=port,
            label_config=label_config,
            log_level=log_level
        )

    def _run_app(self):
        run_app(label_studio_config=self.project_config)

    def change_config(self, data_catalog: DataCatalog):
        self.project_config.source_params['connstr'] = data_catalog.connstr
        self.project_config.source_params['schema'] = data_catalog.schema
        self.project_config.target_params['connstr'] = data_catalog.connstr
        self.project_config.target_params['schema'] = data_catalog.schema

    def process_data(self, data_catalog):
        self.change_config(data_catalog)
        Project._storage = {}  # Clear Project memory
        Project.get_or_create(
            self.project_config.project_name,
            self.project_config,
            context={'multi_session': False}
        )

    def run_services(self):
        if self.process is None:
            logger.info('Start project...')
            self.process = Process(target=self._run_app)
            self.process.start()

    def terminate_services(self):
        if self.process is not None:
            self.process.terminate()
            self.process.join()

    def run(self, data_catalog: DataCatalog, **kwargs):
        self.process_data(data_catalog)
        self.run_services()  # Runs only once

    def __del__(self):
        self.terminate_services()

    @property
    def name(self):
        return f"{type(self).__name__}_{self.project_path.name}"


class Pipeline:
    def __init__(
        self,
        data_catalog: DataCatalog,
        pipeline: List[Union[StoreNode, PythonNode, LabelStudioNode]]
    ):
        self.data_catalog = data_catalog
        self.pipeline = pipeline
        self.transformation_graph = self._get_transformation_graph()

    def run(self, chunksize: int = 1000):
        for node in self.pipeline:
            logger.info(f"Running node '{node.name}'")
            node.run(data_catalog=self.data_catalog, chunksize=chunksize)

    def _get_transformation_graph(self) -> Dict[str, List[str]]:
        transformation_graph = {}
        start_nodes = set()
        for node in self.pipeline:
            if isinstance(node, StoreNode):
                for output in node.outputs:
                    transformation_graph[output] = []
                    start_nodes.add(output)
            elif isinstance(node, PythonNode):
                for output in node.outputs:
                    for input in node.inputs:
                        transformation_graph[input].append(output)
                        transformation_graph[output] = []
            elif isinstance(node, LabelStudioNode):
                input = node.input
                if input not in transformation_graph:
                    transformation_graph[input] = []
                output = node.output
                transformation_graph[input].append(output)
                transformation_graph[output] = []
            else:
                raise ValueError(f"Unknown node: {type(node)}.")

        return transformation_graph
