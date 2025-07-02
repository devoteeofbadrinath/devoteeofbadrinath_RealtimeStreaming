import csv
import logging
import dataclasses as dtc
import typing as tp
from collections.abc import Sequence, Iterable
from operator import itemgetter, attrgetter
from pyspark.sql import SparkSession, Column, DataFrame, functions as F

from .stream_types import StreamConfig, DatabaseType, JoinType, MappingType, \
    SourceDataQuery, ReferenceDataQuery, ColumnMapping, TransformParams, \
    TransformConfig, ApplicationContext, TransformationStep
from brdj_stream_processing.utils.phoenix_db import read_phoenix_table
from brdj_stream_processing.utils.csv import csv_column_index
import sys

quote_col = '`{}`'.format

T = tp.TypeVar('T', SourceDataQuery, ReferenceDataQuery, ColumnMapping)

logger = logging.getLogger(__name__)

def transform_data(
        ctx: ApplicationContext,
        source_df: DataFrame,
        transformation_step: TransformationStep
) -> DataFrame:
    config = ctx.config
    spark = ctx.spark

    params = config.transform_params
    task_id = config.pipeline_task_id

    logger.info(f'executing transformation: task id={task_id}, params={params}')

    if transformation_step == TransformationStep.NORMALISE:
        source_df.createOrReplaceGlobalTempView("stream_df")
        
    reference_df = prepare_reference_data(
        spark, config, ctx.config.transform_cfg.ref_data_query, transformation_step
    )
    
    if transformation_step == TransformationStep.TRANSFORM:
        transformed_df = transform_source_data(
        spark, ctx.config.transform_cfg.source_data_query, reference_df
        )
    
        transformed_df = transform_column_mapping(
        config, ctx.config.transform_cfg.mapping, transformed_df
        )

        logger.info('number of rows after transformations: {}'.format(transformed_df.count()))
        return transformed_df
    elif transformation_step == TransformationStep.NORMALISE:
        return reference_df
    else:
        raise TypeError(f'Unknown transformation type: {transformation_step}')

def read_transform_config(
        params: TransformParams, task_id: str
) -> TransformConfig:
    
    with open(params.reference_data) as f:
        ref_data_cfg = read_reference_data_config(f, task_id, params.reference_schema_mapping)

    with open(params.source_data) as f:
        src_data_cfg = read_source_data_config(f, task_id)
        
    with open(params.mapping) as f:
        mapping_cfg = read_mapping_config(f, task_id)
        
    return TransformConfig(ref_data_cfg, src_data_cfg, mapping_cfg)

def read_mapping_config(file: tp.IO, task_id: str) -> Sequence:
    reader = csv.reader(file)
    header = next(reader)

    indexes = (
        csv_column_index(header, 'pipeline_task_id'),
        csv_column_index(header, 'column_order'),
        csv_column_index(header, 'source_col_name'),
        csv_column_index(header, 'target_col_name'),
        csv_column_index(header, 'target_data_type'),
        csv_column_index(header, 'transformation_rule'),
    )

    schema = str, int, str, str, str, to_transform
    assert len(schema) == len(indexes)

    extract = itemgetter(*indexes)
    to_row = lambda item: [t(v) for t,v in zip(schema, extract(item))]

    items = (ColumnMapping(*to_row(item)) for item in reader)
    rules = query_pipeline_sorted(items, task_id)

    logger.info(f"transformation mappings read: pipeline task = {task_id}, count = {len(rules)}")

    if len(rules) == 0:
        raise ValueError('Cannot find transformation column mapping configuration')
    
    print("rules = ",rules)

    return rules

def read_source_data_config(file: tp.IO, task_id: str) -> SourceDataQuery:
    reader = csv.reader(file)
    header = next(reader)

    indexes = (
        csv_column_index(header, 'pipeline_task_id'),
        csv_column_index(header, 'sql_query'),
        csv_column_index(header, 'primary_key'),
        csv_column_index(header, 'join_type'),
        csv_column_index(header, 'merge_key'),
        csv_column_index(header, 'audit_column'),
    )
    
    to_join = lambda v: JoinType[v.upper()]
    schema = str, str, str, to_join, str, str
    assert len(schema) == len(indexes)

    extract = itemgetter(*indexes)
    to_row = lambda item: [t(v) for t, v in zip(schema, extract(item))]

    items = (SourceDataQuery(*to_row(item)) for item in reader)
    rules = query_pipeline(items, task_id)
    if len(rules) == 0:
        raise ValueError('Cannot find source data query configuration')
    elif len(rules) > 1:
        raise ValueError('Multiple source data query configuration entries')
    else:
        assert len(rules) == 1

    logger.info(
        f'transformation source data query read:'
        f'pipeline task = {task_id}, count{len(rules)}'
    )
    return rules[0]


def read_reference_data_config(file: tp.IO, task_id: str, reference_schema_mapping: dict) -> Sequence:
    reader = csv.reader(file)
    header = next(reader)

    indexes = (
        csv_column_index(header, 'pipeline_task_id'),
        csv_column_index(header, 'query_order'),
        csv_column_index(header, 'sql_query'),
        csv_column_index(header, 'temp_table_name'),
        csv_column_index(header, 'database_type'),
        csv_column_index(header, 'transformation_step'),
    )

    to_db_type = lambda v: DatabaseType[v.upper()]
    to_transformation_step = lambda v: TransformationStep[v.upper()]

    schema = str, int, str, str, to_db_type, to_transformation_step
    assert len(schema) == len(indexes)

    extract = itemgetter(*indexes)
    to_row = lambda item: [t(v) for t, v in zip(schema, extract(item))]

    rows = [to_row(item) for item in reader]

    items = (ReferenceDataQuery(*item) for item in rows)
    rules = query_pipeline_sorted(items, task_id)
    logger.info("reference_schema_mapping = %s", reference_schema_mapping)
    if reference_schema_mapping:
        rules = [update_reference_schema_mapping(rule, reference_schema_mapping) if rule.db_type!=DatabaseType.SPARKSQL else rule for rule in rules]

    logger.info(
        f'transformation reference data read:'
        f' pipeline task={task_id}, count={len(rules)}'
    )
    print("rules =",rules)
    return rules

def prepare_reference_data(
        spark: SparkSession,
        stream_config: StreamConfig,
        config: Sequence,
        transformation_step: TransformationStep,
) -> tp.Optional[DataFrame]:
    df = None
    logger.info("stream_config = %s",stream_config)
    logger.info("stream_config.phoenix_data_source = %s",stream_config.phoenix_data_source)
    #logger.info("stream_config.phoenix_data_source.zkurl = %s",stream_config.phoenix_data_source.zkurl)
    
    for rq in config:
        if rq.transformation_step==transformation_step:
            if rq.db_type == DatabaseType.PHOENIX:
                df = read_phoenix_table(spark, rq.query, ['*'], stream_config.phoenix_data_source["zkurl"])
            elif rq.db_type == DatabaseType.HIVE:
                df = spark.sql(rq.query)
                #df = df.persist()
            elif rq.db_type == DatabaseType.SPARKSQL:
                df = spark.sql(rq.query)
            else:
                raise ValueError('Unknown database type: {}'.format(rq.db_type))
        
            df.createOrReplaceTempView(rq.table)
            logger.info("Showing the dataframe schema after prepare_reference_data %s",df.printSchema())
            df.show()

    return df

def transform_source_data(
        spark: SparkSession,
        config: SourceDataQuery,
        reference_df: tp.Optional[DataFrame]
) -> DataFrame:
    logger.info("config = %s",config)
    if config.join_type != JoinType.NONE and reference_df is None:
        raise ValueError('Join type specified, but no reference data')
    
    df = spark.sql(config.query)
    if config.join_type != JoinType.NONE and reference_df is not None:
        df = df.join(
            reference_df, on=config.merge_key, how=config.join_type.value
        )
    return df

def transform_column_mapping(
        config: StreamConfig,
        col_mapping: Sequence,
        df: DataFrame
) -> DataFrame:
    
    mappings = [(m.target, m) for m in col_mapping]
    columns = [quote_col(m.target) for m in col_mapping]
    transform = {c: map_column(config, m) for c,m in mappings}
    
    return df.withColumns(transform).select(*columns)

def map_column(config: StreamConfig, mapping: ColumnMapping) -> Column:
    if mapping.mapping == MappingType.PASSTHROUGH:
        col = F.col(mapping.source)
    elif mapping.mapping == MappingType.STREAM_PROCESS_NAME:
        col = F.lit(config.stream_process_name)
    else:
        col = F.expr(mapping.mapping)

    return col.cast(mapping.target_type)

def query_pipeline(data: Iterable, task_id: str) -> Sequence:
    return [item for item in data if item.pipeline_task_id == task_id]

def query_pipeline_sorted(data: Iterable, task_id: str) -> Sequence:
    items = query_pipeline(data, task_id)
    return sorted(items, key = attrgetter('order'))

def to_transform(data:str) -> tp.Union[MappingType, str]:
    transform: MappingType | str
    try:
        transform = MappingType[data.upper()]
    except KeyError:
        transform = data
    return transform

def update_reference_schema_mapping(rule: ReferenceDataQuery,
                                    reference_schema_mapping: dict) -> list:    
    try:
        schema, table_name = rule.query.split('.')
    except ValueError as exc:
        raise ValueError("Invalid reference table format: {}. Expected <schema>.<table>.".format(rule.query)) from exc
    
    if reference_schema_mapping.get(schema):
        new_schema = reference_schema_mapping[schema]
        return dtc.replace(rule, query=f'{new_schema}.{table_name}')
    return rule
