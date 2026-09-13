from typing import List, Dict, Optional, Self, Any
from dataclasses import dataclass, field
from enum import Enum

import io
import pyarrow as pa
import pyarrow.parquet as pq
 
from  datapipe_router.pb2.router_client_pb2 import (
    GraphData,
    TableData as MessageTableData,
    PipelineNodeDetails,
    PipelineStepDetail,
    MetaStepDetail,
    LabelsItem,
    TableDetails,
    TableColumn as MessageTableColumn,
    ChangeList as ChangeListMessage
)


class RUN_STATUSES(Enum):
    CREATED = "Created"
    PENDING = "Pending"
    RUNNING = "Running"
    FINISHED = "Finished"
    FAILED = "Failed"
    CANCELED = "Canceled"
    UNKNOWN = "Unknown"


@dataclass
class TableColumn:
    name: str
    type: str

    def to_message(self) -> MessageTableColumn:
        return MessageTableColumn(
            name=self.name,
            type=self.type
        )

    @classmethod
    def from_message(cls, response: MessageTableColumn) -> Self:
        return cls(
            name=response.name,
            type=response.type
        )


@dataclass
class TableNode:
    name: str
    store_class: str
    indexes: List[str]
    schema: List[TableColumn] = field(default_factory=list) 
    size: Optional[int] = None

    @classmethod
    def from_message(cls, response: TableDetails) -> Self:
        return cls(
            name=response.name,
            store_class=response.store_class,
            indexes=list(response.indexes),
            schema=[TableColumn.from_message(column) for column in response.schema],
            size=response.size
        )

    def to_message(self) -> TableDetails:
        return TableDetails(
            name=self.name,
            store_class=self.store_class,
            indexes=self.indexes,
            schema=[column.to_message() for column in self.schema],
            size=self.size
        )
    

@dataclass
class PipelineNode:
    type: str
    transform_type: str
    name: str
    inputs: List[str]
    outputs: List[str]
    labels: List[List[str]]


@dataclass
class PipelineStepNode(PipelineNode):
    has_transform_meta: bool = False
    indexes: Optional[List[str]] = None
    total_idx_count: Optional[int] = None
    changed_idx_count: Optional[int] = None

    @classmethod
    def from_message(cls, response: PipelineStepDetail) -> Self:
        return cls(
            name=response.name,
            type=response.type,
            transform_type=response.transform_type,
            inputs=list(response.inputs),
            outputs=list(response.outputs),
            labels=[list(label.item) for label in response.labels],
            has_transform_meta=response.has_transform_meta,
            indexes=list(response.indexes),
            total_idx_count=response.total_idx_count,
            changed_idx_count=response.changed_idx_count
        )

    def to_message(self) -> PipelineNodeDetails:
        return PipelineNodeDetails(
            pipeline_step=PipelineStepDetail(
                name=self.name,
                type=self.type,
                transform_type=self.transform_type,
                inputs=self.inputs,
                outputs=self.outputs,
                labels=[LabelsItem(item=item) for item in self.labels],
                has_transform_meta=self.has_transform_meta,
                indexes=self.indexes,
                total_idx_count=self.total_idx_count,
                changed_idx_count=self.changed_idx_count
            )
        )

@dataclass
class MetaPipelineStepNode(PipelineNode):
    graph: "Graph"

    @classmethod
    def from_message(cls, response: MetaStepDetail) -> Self:
        return cls(
            name=response.name,
            type=response.type,
            transform_type=response.transform_type,
            inputs=list(response.inputs),
            outputs=list(response.outputs),
            labels=[list(label.item) for label in response.labels],
            graph=Graph.from_message(response.graph)
        )    

    def to_message(self) -> PipelineNodeDetails:
        return PipelineNodeDetails(
            meta_step=MetaStepDetail(
                name=self.name,
                type=self.type,
                transform_type=self.transform_type,
                inputs=self.inputs,
                outputs=self.outputs,
                labels=[LabelsItem(item=item) for item in self.labels],
                graph=self.graph.to_message()
            )
        )


@dataclass
class Graph:
    catalog: Dict[str, TableNode]
    pipeline: List[PipelineNode]
    stages: List[str] = field(default_factory=list) 

    @classmethod
    def from_message(cls, response: GraphData) -> Self:
        pipeline = []

        for node in response.pipeline:
            active_field = node.WhichOneof("details")

            pipeline.append(
                PipelineStepNode.from_message(node.pipeline_step)
                if active_field == "pipeline_step" 
                else MetaPipelineStepNode.from_message(node.meta_step)
            )
            
        return cls(
            catalog={name: TableNode.from_message(node) for name, node in response.catalog.items()},
            pipeline=pipeline,
            stages=list(response.stages)
        )

    def to_message(self) -> GraphData:
        return GraphData(
            catalog={name: node.to_message() for name, node in self.catalog.items()},
            pipeline=[node.to_message() for node in self.pipeline],
            stages=self.stages
        )


@dataclass
class DataFilter:
    data: Dict[str, Any]

    def to_bytes(self):
        buffer = io.BytesIO()
        table = pa.Table.from_pydict({
            k: [v]
            for k, v in self.data.items()
        })

        pq.write_table(table, buffer)
        buffer.seek(0)

        return buffer.read()

    @classmethod
    def from_bytes(cls, data: bytes):
        try:
            buffer = io.BytesIO(data)
            table = pq.read_table(buffer)

            return cls(
                data={
                    k: v[0]
                    for k, v in table.to_pydict().items()
                }
            )
        except:
            return None


@dataclass
class TableData:
    page: int
    page_size: int
    data: pa.Table
    total: Optional[int] = None

    @classmethod
    def from_message(cls, response: MessageTableData) -> Self:
        try:
            buffer = io.BytesIO(response.data)
            data = pq.read_table(buffer)
        except:
            return None
            
        return cls(
            page = response.page,
            page_size = response.page_size,
            data = data,
            total = response.total
        )
    
    def to_message(self) -> MessageTableData:
        buffer = io.BytesIO()
        
        pq.write_table(self.data, buffer)
        buffer.seek(0)

        return MessageTableData(
            page = self.page,
            page_size = self.page_size,
            data = buffer.read(),
            total = self.total
        )


@dataclass
class ChangeList:
    table: str
    index: pa.Table

    @classmethod
    def from_message(cls, response: ChangeListMessage) -> Self:
        try:
            buffer = io.BytesIO(response.index)
            index = pq.read_table(buffer)
        except:
            return None
            
        return cls(
            table = response.table,
            index = index,
        )

    def to_message(self) -> ChangeListMessage:
        buffer = io.BytesIO()
                
        pq.write_table(self.index, buffer)
        buffer.seek(0)

        return ChangeListMessage(
            table = self.table,
            index = buffer.read(),
        )


@dataclass
class Event:
    timestamp: float


@dataclass
class StatusEvent(Event):
    status: str


@dataclass
class LogEvent(Event):
    sequence: int
    lof: str