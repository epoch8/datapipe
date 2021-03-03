import logging
from abc import abstractmethod

from typing import Any, Callable, Dict, Iterator, List, Tuple
from c12n_pipe.io.data_catalog import DataCatalog
from c12n_pipe.datatable import gen_process_many, inc_process_many
import pandas as pd


logger = logging.getLogger('c12n_pipe.node')


class Node:
    def run(self, data_catalog: DataCatalog, **kwargs):
        pass

    def run_services(self, data_catalog: DataCatalog, **kwargs):
        pass

    def terminate_services(self, data_catalog: DataCatalog, **kwargs):
        pass

    def heavy_run(self, data_catalog: DataCatalog, **kwargs):
        pass

    @property
    def inputs(self):
        return []

    @property
    def outputs(self):
        return []

    @property
    def name(self):
        return f"{type(self).__name__}"


class StoreNode(Node):
    def __init__(
        self,
        proc_func: Callable[[], Iterator[Tuple[pd.DataFrame, ...]]],
        outputs: List[str],
        kwargs: Dict[str, Any] = {}
    ):
        self.proc_func = proc_func
        self.kwargs = kwargs
        self._outputs = outputs

    def run(self, data_catalog: DataCatalog, **kwargs):
        outputs_dt = [data_catalog.get_data_table(output) for output in self.outputs]
        gen_process_many(
            dts=outputs_dt,
            proc_func=self.proc_func,
            **self.kwargs
        )

    @property
    def inputs(self):
        return []

    @property
    def outputs(self):
        return self._outputs

    @property
    def name(self):
        return self.proc_func.__name__


class PythonNode(Node):
    def __init__(
        self,
        proc_func: Callable,
        inputs: List[str],
        outputs: List[str],
        kwargs: Dict[str, Any] = {}
    ):
        self.proc_func = proc_func
        self._inputs = inputs
        self._outputs = outputs
        self.kwargs = kwargs

    def run(
        self,
        data_catalog: DataCatalog,
        chunksize: int = 1000,
        **kwargs
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
    def inputs(self):
        return self._inputs

    @property
    def outputs(self):
        return self._outputs

    @property
    def name(self):
        return self.proc_func.__name__


class Pipeline:
    def __init__(
        self,
        data_catalog: DataCatalog,
        pipeline: List[Node]
    ):
        self.data_catalog = data_catalog
        self.pipeline = pipeline
        self.transformation_graph = self._get_transformation_graph()

    def run(self, **kwargs):
        for node in self.pipeline:
            logger.info(f"Running node '{node.name}'")
            node.run(data_catalog=self.data_catalog, **kwargs)

    def heavy_run(self, **kwargs):
        for node in self.pipeline:
            logger.info(f"Running heavy node '{node.name}'")
            node.heavy_run(data_catalog=self.data_catalog, **kwargs)

    def run_services(self, **kwargs):
        for node in self.pipeline:
            logger.info(f"Starting services of node '{node.name}'")
            node.run_services(data_catalog=self.data_catalog, **kwargs)

    def terminate_services(self, **kwargs):
        for node in self.pipeline:
            logger.info(f"Terminate services of node '{node.name}'")
            node.terminate_services(data_catalog=self.data_catalog, **kwargs)

    def _get_transformation_graph(self) -> Dict[str, List[str]]:
        transformation_graph = {}
        for node in self.pipeline:
            assert isinstance(node, Node), f"Unknown node: {type(node)}"
            for input in node.inputs:
                if input not in transformation_graph:
                    transformation_graph[input] = []
                for output in node.outputs:
                    if output not in transformation_graph[input]:
                        transformation_graph[input].append(output)
        return transformation_graph
