from __future__ import annotations

import typing as tp
from dataclasses import dataclass, field
from typing import Dict
from datetime import datetime, timezone
from queue import Queue
from pyspark.sql import SparkSession,DataFrame
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
    Parameters for phoenix datasource configuration.
    
    Attributes:
        :param schema: Name of the schema.
        :param offset_table: Name of the offset table.
        :param stats_table: Name of the stats table.
        :param zkurl: Zookeeper URL for phoenix connection.
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
class LogParams:
    """
    Parameters for Logging configuration.

    :param log_level: Minimum level of recorded logs.
    :param log_path: Path to where application logs are stored.
    :param run_id: Identifier of the current pipeline run.
    """
    log_level: str
    log_path: str
    run_id: str

@dataclass(frozen=True)
class ValidationParams:
    """
    Data validation params
    """
    config_path: str


@dataclass(frozen=True)
class StreamConfig:
    """Stream pipeline configuration."""
    pipeline_task_id: str
    pipeline_name: str
    stream_process_name: str
    user_id: str
    workload_password: str
    kafka_topic: str
    kafka_topic_dlq: str
    kafka_bootstrap_servers: str
    s3_log_bucket: str
    s3_endpoint: str
    application_shutdown_time: int
    offset_sleeping_time: int
    schema_registry_details: SchemaRegistry
    phoenix_data_source: PhoenixDataSource
    dag_pipeline_args: dict
    log_params: LogParams
    validation_params: ValidationParams
    transform_params: TransformParams
    transform_cfg: TransformConfig


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

class ValidationType(Enum):
    POST_TRANSFORM = "post_transform"
    PRE_TRANSFORM = "pre_transform"
    BOTH = "both"

class TransformationStep(Enum):
    NORMALISE = "normalise"
    TRANSFORM = "transform"

@dataclass(frozen=True)
class PipelineData:
    """
    Data and it's metadata as processed by a pipeline.
    
    :var data: Data read from a data source by the input module.
    :var rejected: Number of rejected rows by validation module.
    """
    data: DataFrame
    rejected: int = 0

class ValidationName(Enum):
    IS_MANDATORY = "is_mandatory"
    IS_VALID_DATE = "is_valid_date"
    IS_ENUM = "is_enum"
    IS_INTEGER = "is_integer"
    IS_FLOAT = "is_float"
    VALIDATION_KEY = "validation_key"

@dataclass(frozen=True)
class ValidationRecordColumn:
    """
    Validation of records configuration for a dataframe column.
    
    :var col_name: Dataframe column name or list of column names.
    :var source_date_format: Date format if validation is for a timestamp column.
    :var enum_value: List of values accepted as column values.
    :var validation_name: Kind of validation, i.e. are column values a date or
        integer values.
    """
    col_name: str | list[str]
    source_date_format: str
    enum_value: list[tp.Union[str, None]]
    validation_name: ValidationName

@dataclass(frozen=True)
class ValidationRecordConfig:
    """
    Configuration for validation of records.

    :var pipeline_name: Name of the stream pipeline.
    :var pipeline_task_id: Stream pipeline task id.
    :var validation_type: Validation type - pre-transform or post-transform
        validation.
    :var key: Key columns of the validation.
    :var columns: Columns to validate.
    """
    pipeline_name: str
    pipeline_task_id: str
    validation_type: ValidationType
    key: list[str]
    columns: list[ValidationRecordColumn]

class DatabaseType(Enum):
    HIVE = "hive"
    PHOENIX = "phoenix"
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
    transformation_step: TransformationStep

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

