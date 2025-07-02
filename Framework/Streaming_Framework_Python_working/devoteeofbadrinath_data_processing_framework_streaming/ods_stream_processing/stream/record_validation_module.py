"""
Validation of records module.

Validation of records is peformed for a specific validation type

    `pre-transform`
        Validation of records before transformation of source data is done.
    `post-transform`
        Validation of records after transformation of source data is done.

Validation of records consists of the following steps

- read and parse configurations
- analyze data and attach validation of records metadata
- summarize rejected records
- check results of validation of records, this is if limit of number of
  rejected records is not breached.
- store results of validation of records (save rejected records and it's
  summary in a database)

The important objects and functions

    `validate_records`
        Main entry function to read configuration, perform validation of
        records, check the results of validation, and store the results of
        validation.

    `read_validation_config`
        Read and parse configuration of validation of records.

    `_store_validation_results`
        Store results of validation of records (rejected records and its summary).

    `VALIDATION_FUNCTIONS`
        List of functions to validate data.
"""

from __future__ import annotations

import csv
import dataclasses as dtc
import logging
import operator as op
import typing as tp
from operator import itemgetter
from collections.abc import Iterable
from functools import partial, reduce
from itertools import chain
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql import functions as F
from brdj_stream_processing.stream.stream_types import ApplicationContext, PipelineData, \
    ValidationType, ValidationName, ValidationRecordConfig, ValidationRecordColumn
from brdj_stream_processing.stream.stream_config import StreamConfig
from brdj_stream_processing.stream.output_module import write_to_kafka
from brdj_stream_processing.utils.csv import csv_column_index

logger = logging.getLogger(__name__)

# regular expression to check if string is an integer, but *not* float
# number; we are trying to follow Spark rules here, so whitespace at the
# start and end is allowed
# note: with Spark 3.5.0 we might want to use `rlike` or `to_number`
# function in the future
RE_IS_INT = r'^\s*[-+]?\d*\s*$'

# regular expression to check if string is a float number
RE_IS_FLOAT = r'[-+]?\d*\.\d+(?:[eE][-+]?\d+)?'

# column with metadata for validation of records
COL_META = '_meta'

VALIDATION_MESSAGE = {
    ValidationName.IS_MANDATORY: 'Mandatory column contains null value',
    ValidationName.IS_VALID_DATE: 'Date format is not valid',
    ValidationName.IS_ENUM: 'Invalid enum values',
    ValidationName.IS_INTEGER: 'Integeer field contains non-integer valie',
    ValidationName.IS_FLOAT: 'Float field contains non-float value',
}

quote_col = '`{}`'.format

flatten = chain.from_iterable


#
# functions creating Spark expressions to check data
#

def is_integer(column: str) -> Column:
    """
    Check the column values are integer or null values.

    :param column: Column name.
    """
    return F.col(column).isNull() | (F.regexp_extract(column, RE_IS_INT, 0) != '')


def is_float(column: str) -> Column:
    """
    check the column values are float or null values.

    :param column: Column name.
    """
    return F.col(column).isNull() | (F.regexp_extract(column, RE_IS_FLOAT, 0) != '')


def is_enum(column: str, values: list[str]) -> Column:
    """
    Check the column to contain enumerated values.
    
    :param column: Column name.
    :param values: List of enumerated values.
    """
    if None in values:
        values = list(filter(None, values))
        check = F.col(column).isin(values) | F.col(column).isNull()
    else:
        check = F.col(column).isin(values) & ~F.col(column).isNull()
    return check


def is_mandatory(column: str) -> Column:
    """
    Check the column contains non-null values.
    
    :param column: Column name.
    """
    return F.col(column).isNotNull()


def is_date(column: str, date_fmt: str) -> Column:
    """
    Check the column contains dates in the specified format or null values.
    
    :param column: Column name.
    :param date_fmt: Expected date format.
    """
    return F.to_date(F.col(column), date_fmt).isNotNull() | F.col(column).isNull()


VALIDATION_FUNCTIONS = {
    ValidationName.IS_MANDATORY: is_mandatory,
    ValidationName.IS_VALID_DATE: is_date,
    ValidationName.IS_ENUM: is_enum,
    ValidationName.IS_INTEGER: is_integer,
    ValidationName.IS_FLOAT: is_float,
}


def validate_records(
        ctx: ApplicationContext,
        pipeline_data: PipelineData,
        validation_type: ValidationType,
) -> PipelineData:
    """
    Validate records.
    
    :param ctx: Application context.
    :param pipeline_data: Pipeline data to validate.
    :param validation_type: Valdation type - pre-transform or post-transform.
    """
    stream_config = ctx.config
    spark = ctx.spark
    config = read_validation_config(
        stream_config.validation_params.config_path,
        stream_config.pipeline_name,
        stream_config.pipeline_task_id,
        validation_type,
    )
    print("Config = ", config)
    # no configuration, so return the input data
    if not config.key:
        logger.info("BRDJ BRDJ")
        #df.printSchema()
        return pipeline_data
    
    # validate records and summarize rejected records
    #valid, rejected = _validate_records_meta(spark, config, pipeline_data.data)
    valid = _validate_records_meta(spark, config, pipeline_data.data)

    #valid = valid.persist()
    #rejected = rejected.persist()
    #rejected_count = rejected.count() + pipeline_data.rejected

    # drop validation metadata
    # valid = valid.drop('_meta')
    # rejected = rejected.drop('_meta')

    # _write_rejected_records_to_dlq(rejected, ctx.config)

    return dtc.replace(pipeline_data, data=valid, rejected=0)
    #return dtc.replace(pipeline_data, data=valid, rejected=rejected_count)


#
# read and parse configuration of validation of records
#
def read_validation_config(
        path: str,
        pipeline_name: str,
        task_id: str,
        validation_type: ValidationType,
) -> ValidationRecordConfig:
    """
    Read configuration of validation of records from a CSV file.

    :param path: Path to CSV file with the configuration.
    :param pipeline_name: Stream pipeline name.
    :param pipeline_task_id: Stream pipeline task id.
    :param validation type Validation type - pre-transform or
        post-transform
    """
    with open(path, encoding='utf-8-sig') as f:
        config = parse_validation_config(f, pipeline_name, task_id, validation_type)
    return config


def parse_validation_config(
        file: tp.IO,
        pipeline_name: str,
        task_id: str,
        validation_type: ValidationType
) -> ValidationRecordConfig:
    """
    Parse configuration of validation of records.
    
    :param file: Input file stream.
    :param pipeline_name: Stream pipeline name.
    :param task_id: Stream pipeline task id.
    :param validation_type: Type of record validation to perform.
    """
    reader = csv.reader(file)
    header = next(reader)

    indexes = (
        csv_column_index(header, 'pipeline_task_id'),
        csv_column_index(header, 'validation_type'),
        csv_column_index(header, 'col_name'),
        csv_column_index(header, 'source_date_format'),
        csv_column_index(header, 'enum_value'),
        csv_column_index(header, 'validation_name'),
    )

    # NOTE: note the order of columns above
    to_validation_type = lambda v: ValidationType[v.upper()]
    to_validation_name = lambda v: ValidationName[v.upper()]
    schema = str, to_validation_type, str, str, to_enum, to_validation_name
    assert len(schema) == len(indexes)

    extract = itemgetter(*indexes)
    # extract data from CSV line, and apply type from schema
    to_row = lambda item: [t(v) for t, v in zip(schema, extract(item))]

    # read configurations and find configuration items for specific stream
    # pipeline and validation type
    rows = [to_row(item) for item in reader]
    rows = [
        item for item in rows
        if item[0] == task_id and item[1] in (validation_type, validation_type.BOTH)
    ]

    # determine validation key and validation columns
    all_col = [ValidationRecordColumn(*r[2:]) for r in rows]
    key_items = (c for c in all_col if c.validation_name == ValidationName.VALIDATION_KEY)
    key = next(key_items, None)
    if key:
        key_cols = [v.strip() for v in key.col_name.split(',')]
    else:
        key_cols = []

    columns = [c for c in all_col if c.validation_name != ValidationName.VALIDATION_KEY]

    if not key_cols and columns:
        raise ValueError('Validation key not found')
    
    config = ValidationRecordConfig(
        pipeline_name,
        task_id,
        validation_type,
        key_cols,
        columns
    )

    logger.info(
        f'record validation configuration data read:'
        f'pipeline task={task_id}, validation_type={validation_type}, count={len(columns)}'
    )
    return config


def to_enum(value: str) -> list:
    if not value.strip():
        return []
    items = [v.strip() for v in value.split(',')]
    items = [v if v else None for v in items]
    return items


#
# validate data and summarize validation
#
def _validate_records_meta(
        spark: SparkSession, config: ValidationRecordConfig, df: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """
    Add metadata of validation of records into dataframe with input data.

    :param spark: Spark session.
    :param config: Configuration of validation of records.
    :param df: Input data.
    """
    if config.columns:
        # Create nested structure with validation information for each
        # column; support dataset columns with multiple configuration
        # entries
        #
        #      struct(
        #           col_a_0=struct(validation='is_integer|is_float|is_mandatory|...', valid=True|False)
        #           col_b_1=struct(validation='is_integer|is_float|is_mandatory|...', valid=True|False)
        #           col_b_2=struct(validation='is_integer|is_float|is_mandatory|...', valid=True|False)
        #           ...
        #           col_z_n=struct(validation='is_integer|is_float|is_mandatory|...', valid=True|False)
        #       )
        #
        # print("BRDJ",list(_meta_columns(config)))
        # for cfg, ck in _meta_columns(config):
        #    print(f"Checking _ctor_validator output for {ck}: {type(_ctor_validator(cfg))}")
        
        # meta = []
        # for cfg, ck in _meta_columns(config):
        #     valid_col = _ctor_validator(cfg)
        #     if not isinstance(valid_col, col):
        #         print(f"Warning: _ctor_validator(cfg) returned {type(valid_col)} for column {ck}")

        #     meta.append(
        #     struct(
        #         lit(cfg.validation_name.value).alias("validation"),
        #         col(valid_col).alias("valid"),  # Ensure it's a Column
        #     ).alias(ck)
        #     )
    
        meta = [
            F.struct(
                F.lit(cfg.validation_name.value).alias('validation'),
                _ctor_validator(cfg).alias('valid'),
            ).alias(ck)
            for cfg, ck in _meta_columns(config)
        ]
        # print("meta = ", meta)
        # print("type of data frame ",type(df))
        # print("data frame ",df)
        # df = DataFrame(df, spark)
        # print("type of data frame ",type(df))
        # print("data frame ",df)
        
        #df = df.select('*', F.struct(*meta).alias(COL_META)).persist()
        df = df.select('*', F.struct(*meta).alias(COL_META))
        meta_columns = (_meta_attr(ck, 'valid') for _, ck in _meta_columns(config))
        is_valid = reduce(op.and_, meta_columns) # true if all valid

        valid = df.withColumn("_meta_valid", is_valid)
        valid = valid.filter(F.col("_meta_valid"))
        #valid = df.filter(is_valid)
        #rejected = df.filter(~is_valid)
    else:
        valid = df
        rejected = spark.createDataFrame([], df.schema)
    #return valid, rejected
    return valid


#
# utility functions
#


def _ctor_validator(cfg: ValidationRecordColumn) -> Column:
    """
    Create validator Spark expression for a dataframe column.

    :param cfg: Validation of records configuration for a dataframe column.
    """
    func = VALIDATION_FUNCTIONS[cfg.validation_name]
    if cfg.validation_name == ValidationName.IS_VALID_DATE:
        func = partial(func, date_fmt=cfg.source_date_format)
    elif cfg.validation_name == ValidationName.IS_ENUM:
        func = partial(func, values=cfg.enum_value)

    col = cfg.col_name
    # return func(quote_col(col)).alias('valid')
    return func(col).alias('valid')


def _meta_columns(config: ValidationRecordConfig) -> Iterable[tuple[ValidationRecordColumn, str]]:
    """
    Generate unique column names for metadata of validation of records.
    
    A single column of a dataset can have multiple validation of records
    configuration entries, for example `is integer` and `is mandatory`. The
    function generates unique column names, so we can distinguish between
    different configurations at the lebel of the metadata.
    
    :param config: Validation of records configuration.
    """
    columns = [
        '{}_{}'.format(cfg.col_name, i) for i, cfg in enumerate(config.columns)
    ]
    return zip(config.columns, columns)


def _meta_attr(column: str, attr: str) -> Column:
    """
    Get attribute of metadata of validation of records.
    
    :param column: Unique column name of metadata of validation of records.
    :param attr: Attribute name of metadata of validation of records.
    """
    return F.col('{}.{}.{}'.format(COL_META, quote_col(column), attr))


def _write_rejected_records_to_dlq(rejected: DataFrame, conf: StreamConfig) -> None:
    """
    Write rejected records to DLQ topic.
    
    :param rejected: Spark dataframe with rejected records.
    :param conf: Stream config variables.
    """
    if not rejected.isEmpty():
        logger.info("Saving rejected records to DLQ topic: %s", conf.kafka_topic_dlq)
        dlq_df = rejected
        dlq_df = dlq_df.withColumn("value", F.to_json(F.struct(*dlq_df.columns))).select("value")
        write_to_kafka(dlq_df, conf, conf.kafka_topic_dlq)
        
