from __future__ import annotations

import typing as tp
from collections.abc import Sequence
from dataclasses import dataclass
from enum import auto, Enum
from typing_extensions import TypedDict
from pyspark.sql import DataFrame, SparkSession
from datetime import datetime

class FileFormat(Enum):
    CSV = auto()
    PARQUET = auto()
    DATA = auto()
    CTL = auto()

class ValidationName(Enum):
    IS_MANDATORY = "is_mandatory"
    IS_VALID_DATE = "is_valid_date"
    IS_ENUM = "is_enum"
    IS_INTEGER = "is_integer"
    IS_FLOAT = "is_float"
    VALIDATION_KEY = "validation_key"

class ValidationType(Enum):
    POST_TRANSFORM = "post_transform"
    PRE_TRANSFORM = "pre_transform"
    BOTH = "both"

class MappingType(Enum):
    PASSTHROUGH = 'passthrough'
    BATCH_PROCESS_NAME = 'batch_process_name'

class RecordValidationError(Exception):
    pass

class JoinType(Enum):
    LEFT = "left"
    RIGHT = "right"
    UNION = "union"
    NONE = "none"

class DatabaseType(Enum):
    HIVE = "hive"
    PHEONIX = "pheonix"
    S3 = "s3"

class SourceType(Enum):
    FILE = "file"
    DATABASE = "database"

class EnabledFlag(Enum):
    Y = True
    N = False

class AppStage(Enum):
    DATA_PROCESSING = "data_processing"
    INPUT = "input"
    FILE_VALIDATION = "file_validation"
    PRE_RECORD_VALIDATION = "pre_transform_record_validation"
    TRANSFORMATION = "transformation"
    POST_RECORD_VALIDATION = "post_transform_record_validation"
    OUTPUT = "output"
    POST_PROCESSING = "post_processing"
    AUDIT_TASK = "audit_task"

class FeatureParams(TypedDict):
    data_processing: bool
    input: bool
    file_validation: bool
    output: bool
    past_processing: bool

@dataclass(frozen=True)
class TransformParams:
    reference_data: str
    source_data: str
    mapping: str
    reference_schema_mapping: dict

@dataclass(frozen=True)
class LogParams:
    log_level: str
    log_path: str
    run_id: str

class FileRejectReason(Enum):
    DATA_EMPTY_CTL_ZERO = ('Data file is empty and CTL file '
                            'says 0 records for processing')

    DATA_EMPTY_CTL_NONZERO = ('Data file is empty but '
                              'CTL file has record counts')
    
    DATA_PRESENT_CTL_ZERO = 'CTL file is empty but data has records'
    DATA_GREATER_THAN_CTL = 'Data record count is greater then CTL file'
    DATA_LESS_THAN_CTL = 'Data record count is less than CTL file'

@dataclass(frozen=True)
class DataOutput:
    table: str
    zkurl: str
    connector: str

@dataclass(frozen= True)
class ValidationParams:
    config_path: str
    rejected_records_limit: int

@dataclass(frozen=True)
class DataSource:
    source_type: SourceType

@dataclass(frozen=True)
class CSVDataSource(DataSource):
    format: FileFormat
    path: str
    delimiter: str
    ctl_path: str
    processed_path: str
    rejected_path: str
    processed_ctl_path: str
    rejected_ctl_path: str
    rejected_records_path: str
    file_mode: str
    escape_chr: str
    multiline_rows: bool

@dataclass(frozen=True)
class PheonixDataSource(DataSource):
    table: str
    zkurl: str
    connector: str

@dataclass(frozen=True)
class BatchConfig:
    feature_enabled: FeatureParams
    batch_process_name: str
    pipeline_name: str
    pipeline_task_id: str
    task_id: str
    log_params: LogParams
    validation_params: ValidationParams
    transform_params: TransformParams
    data_source: DataSource
    data_output: DataOutput

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

@dataclass(frozen=True)
class ValidationRecordColumn:
    col_name: str | list[str]
    source_date_format: str
    enum_value: list[tp.Union[str, None]]
    validation_name: ValidationName

@dataclass(frozen=True)
class ValidationRecordConfig:
    pipeline_name: str
    pipeline_task_id: str
    validation_type: ValidationType
    key: list[str]
    columns: list[ValidationRecordColumn]

@dataclass(frozen=True)
class SourceDataControl:
    bus_data: datetime
    file_date: datetime
    sequence: int
    count: int

@dataclass(frozen=True)
class ApplicationContext:
    config: BatchConfig
    spark: SparkSession
    generic_args: dict
    job_args: dict
    dag_args: dict

@dataclass(frozen=True)
class PipelineData:
    source_type: SourceType
    data: DataFrame
    count: int
    source_control: tp.Optional[SourceDataControl] = None
    rejected: int = 0





