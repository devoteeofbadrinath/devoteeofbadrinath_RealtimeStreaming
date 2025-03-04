from dataclasses import dataclass, field
from typing import Dict
import typing as tp
from datetime import datetime, timezone
from queue import Queue
from pyspark.sql import SparkSession
from enum import auto, Enum
from collections.abc import Sequence


@dataclass(frozen=True)
class TransformParams:
    reference_data: str
    source_data: str
    mapping: str
    reference_schema_mapping: dict


@dataclass(frozen=True)
class SchemaRegistry:
    """
    Pamateres for schema registry configuration.
    
    :param subject: Subject name of the schema.
    :param url: APU url of schema registry.
    """
    subject: str
    url: str


@dataclass(frozen=True)
class PhoenixDataSource:
    """
    Parameters for Pheonix datasource configuration.
    
    Attributes:
        :param schema: Name of the schema.
        :param offset_table: Name of the offset table.
        :param stats_table: Name of the stats table.
        :param zkurl: Zookeeper URL for Pheonix connection.
        :param table: Name of the table.
        :param src_id_col: Source ID column name.
        :param tgt_id_col: Target ID column name.
    """
    schema: str
    offset_table: str
    stats_table: str
    zkurl: str
    table: str
    src_id_col: str
    tgt_id_col: str


@dataclass(frozen=True)
class StreamConfig:
    """Stream pipeline configuration."""
    pipeline_task_id: str
    user_id: str
    workload_password: str
    kafka_topic: str
    kafka_bootstrap_servers: str
    application_shutdown_time: int
    offset_sleeping_time: int
    schema_registry_details: SchemaRegistry
    phoenix_data_source: PhoenixDataSource
    dag_pipeline_args: dict
    transform_params: TransformParams

@dataclass(frozen=True)
class RecordMetrics:
    """
    Class to manage stream metrics.
    """
    metrics_window_start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metrics_window_end_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    total: int = 0
    validation: int = 0
    transformation: int = 0
    exception: int = 0
    dlq: int = 0
    target_write: int = 0
    starting_offsets: Dict[str, int] = field(default_factory=dict)


@dataclass
class ApplicationState:
    """
    Current, internal state of the application.
    
    :var metrics_queue: Queue to maintain offsets and metrics.
    :var is_running: Boolean to flag stream run.
    """
    metrics_queue: Queue
    is_running: bool


@dataclass(frozen=True)
class ApplicationContext:
    """
    Application context for stream pipeline.
    
    :var config: Stream pipeline configuration.
    :var spark: Spark session for pipeline run.
    :var generic_args: Generic arguments for pipeline run.
    :var job_args: Job-specific arguments for pipeline run.
    :var dag_piieline_args: DAG-specific arguments for pipeline run.
    """
    config: StreamConfig
    spark: SparkSession
    generic_args: dict
    job_args: dict
    dag_pipeline_args: dict
    state: ApplicationState

class DatabaseType(Enum):
    HIVE = "hive"
    PHEONIX = "pheonix"
    SPARKSQL = "sparksql"

class JoinType(Enum):
    LEFT = "left"
    RIGHT = "right"
    UNION = "union"
    NONE = "none"

class MappingType(Enum):
    PASSTHROUGH = 'passthrough'
    STREAM_PROCESS_NAME = 'stream_process_name'
    
@dataclass(frozen=True)
class ReferenceDataQuery:
    pipeline_task_id: str
    order: int
    query: str
    table: str
    db_type: DatabaseType

@dataclass(frozen=True)
class SourceDataQuery:
    pipeline_task_id: str
    query: str
    primary_key: str
    join_type: JoinType
    merge_key: str
    audit_column: str


@dataclass(frozen=True)
class ColumnMapping:
    pipeline_task_id: str
    order: int
    source: str
    target: str
    target_type: str
    mapping: tp.Union[MappingType, str]

@dataclass(frozen=True)
class TransformConfig:
    ref_data_query: Sequence
    source_data_query: SourceDataQuery
    mapping: Sequence

