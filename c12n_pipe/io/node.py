import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Set, Union
from multiprocessing import Process

from c12n_pipe.io.catalog import DataCatalog
from c12n_pipe.datatable import inc_process
from c12n_pipe.label_studio_utils.label_studio_c12n import LabelStudioConfig, run_app
from label_studio.project import Project


logger = logging.getLogger('c12n_pipe.node')


class StoreNode:
    def __init__(
        self,
        proc_func: Callable,
        kwargs: Dict[str, Any],
        outputs: List[str]
    ):
        self.proc_func = proc_func
        self.kwargs = kwargs
        self.outputs = outputs

    def run(self, catalog: DataCatalog, chunksize: int = 1000):
        outputs_dt = [catalog.get_data_table(output) for output in self.outputs]
        for i, output_dt in enumerate(outputs_dt):
            df_i = self.proc_func(**self.kwargs)
            if len(outputs_dt) > 1:
                df_i = df_i[i]
            output_dt.store(df=df_i)


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
        catalog: DataCatalog,
        chunksize: int = 1000
    ):
        inputs_dt = [catalog.get_data_table(input) for input in self.inputs]
        outputs_dt = [catalog.get_data_table(output) for output in self.outputs]
        for i, output_dt in enumerate(outputs_dt):
            if len(outputs_dt) > 1:
                def proc_func_i(*args, **kwargs):
                    return self.proc_func(*args, **kwargs)[i]
            else:
                proc_func_i = self.proc_func

            inc_process(
                ds=catalog.data_store,
                input_dts=inputs_dt,
                res_dt=output_dt,
                proc_func=proc_func_i,
                chunksize=chunksize,
                **self.kwargs
            )


class LabelStudioNode:
    def __init__(
        self,
        project_path: Union[str, Path],
        input: str,
        output: str,
        port: int,
        label_config: str,
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

    def change_config(self, catalog: DataCatalog):
        self.project_config.source_params['connstr'] = catalog.connstr
        self.project_config.source_params['schema'] = catalog.schema
        self.project_config.target_params['connstr'] = catalog.connstr
        self.project_config.target_params['schema'] = catalog.schema

    def run(self, catalog: DataCatalog, **kwargs):
        self.change_config(catalog)
        Project.get_or_create(
            self.project_config.project_name,
            self.project_config,
            context={'multi_session': False}
        )
        if self.process is None:
            logger.info('Start project...')
            self.process = Process(target=self._run_app)
            self.process.start()

    def __del__(self):
        if self.process is not None:
            self.process.terminate()
            self.process.join()


class Pipeline:
    def __init__(
        self,
        catalog: DataCatalog,
        pipeline: List[Union[StoreNode, PythonNode]]
    ):
        self.catalog = catalog
        self.pipeline = pipeline
        self.transformation_graph = self._get_transformation_graph()

    def run(self, chunksize: int = 1000):
        for node in self.pipeline:
            if hasattr(node, 'proc_func'):
                node_name = node.proc_func.__name__
            else:
                node_name = type(node).__name__
            logger.info(f"Running node '{node_name}'")
            node.run(catalog=self.catalog, chunksize=chunksize)

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
        self._assert_no_loops(transformation_graph, start_nodes)

        return transformation_graph

    def _assert_no_loops(
        self,
        transformation_graph: Dict[str, List[str]],
        start_nodes: Set[str]
    ):
        def dfs(start, visited=None):
            if visited is None:
                visited = set()
            assert start not in visited, f"Transformation graph has loop (at variable '{start}')"
            visited.add(start)
            for next in set(transformation_graph[start]) - visited:
                dfs(next, visited)
            return visited

        for start_node in start_nodes:
            dfs(start_node)
