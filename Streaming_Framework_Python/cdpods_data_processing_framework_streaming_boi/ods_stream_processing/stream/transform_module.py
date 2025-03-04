import csv
import logging
import dataclasses as dtc
import typing as tp
from collections.abc import Sequence, Iterable
from operator import itemgetter, attrgetter
from pyspark.sql import SparkSession, Column, DataFrame, functions as F

from .stream_types import StreamConfig, DatabaseType, JoinType, MappingType, \
    SourceDataQuery, ReferenceDataQuery, ColumnMapping, TransformParams, \
    TransformConfig, ApplicationContext
import sys

#from ..utils import csv_column_index, read_pheonix_table, quote_col

T = tp.TypeVar('T', SourceDataQuery, ReferenceDataQuery, ColumnMapping)

logger = logging.getLogger(__name__)

def read_pheonix_table(
        spark: SparkSession,
        table: str,
        columns: Sequence,
        zk_url: str,
) -> DataFrame:
    df = spark.read.format('pheonix') \
            .option('table', table) \
            .option('zkurl', zk_url) \
            .load()
    
    return df.select(*columns)

def csv_column_index(header: list, column:str) -> int:

    logger.debug('header: {}'.format(header))
    try:
        return header.index(column)
    except ValueError as ex:
        raise ValueError(f'Column not found: {column}') from ex

def transform_data_structure(df: DataFrame):
    """
    Transform the data into required structure.
    
    :param df: DataFrame containing the data to be transformed.
    """

    # TODO - THE BELOW LINES OF CODE SHOULD BE TAKEN AS A SQL QUERY
    # FROM THE CONFIG FILE AS PART OF THE NEXT SPRINT

    df = (
        df.select("MessageHeader.*", "MessageBody.*", "partition", "offset")
    )
    df = (
        df.select(
            "*",
            "AccArr.*",
            "Branch.*",
        ).drop("AccArr", "Branch")
    )
    return df


def transform_data_old(existing_df: DataFrame, new_df: DataFrame):
    """
    Write data to Pheonix table with UPSERT, using lookup for ID.
    
    :param: existing_df: DataFrame containing the existing data in the pheonix table.
    :param: new_df: DataFrame containing the new data to be written.
    """

    # TODO - THE BELOW LINES OF CODE SHOULD BE TAKEN AS A SQL QUERY
    # FROM THE CONFIG FILE AS PART OF THE NEXT SPRINT

    target_columns = existing_df.columns

    join_condition = F.expr("""
                             ACNT_ID_NUM = CASE 
                             WHEN OriSou = 'BKK'
                             THEN concat('BKKG', '^', AccNum, '^', NSC)
                             END
                             """)
    
    # TODO - WE CANNOT INCLUDE THE BELOW CONDITION SINCE
    # THE SeqNum IS SOME RANDOM DIGIT AT THE MOMENT,
    # HENCE POSSIBILITY OF RETURNING NULL RECORDS AFTER JOIN.
    # CAN BE PART OF NEXT SPRINT OR WHEN PROPER DATA IS POPULATED.

    # join_condition = expr("""
    #       ACNT_ID_NUM = CASE
    #           WHEN OriSou = 'BKK'
    #           THEN concat('BKKG', '^', AccNum, '^', NSC)
    #       END
    #       AND
    #       CASE 
    #           WHEN BalanceStatus = 'SHADOW'
    #               THEN Timestamp > SHDW_BAL_DTTM
    #               AND SeqNum > SHDW_BAL_SEQ
    #           WHEN BalanceStatus = 'POSTED'
    #               THEN Timestamp > LDGR_BAL_DTTM
    #               AND SeqNum > LDGR_BAL_SEQ
    #       END
    # """)

    final_df = new_df.join(
        existing_df,
        join_condition,
        "left"
    )

    final_df = (
        final_df
        .withColumn("ACNT_ROLE_UPDT_SEQ", F.lit(None).cast("string"))
        .withColumn("ACNT_ROLE_UPDT_DTTM", F.lit(None).cast("timestamp"))
        .withColumn("SHDW_BAL_AMT",
                    F.when(
                        F.col("BalamceStatus") == "SHADOW", F.col("Balance")
                    ).otherwise(F.lit(None).cast("decimal(23,4)")))
        .withColumn("SHDW_BAL_DTTM",
                    F.when(
                        F.col("BalamceStatus") == "SHADOW", F.col("Timestamp")
                    ).otherwise(F.lit(None).cast("timestamp")))
        .withColumn("SHDW_BAL_AMT",
                    F.when(
                        F.col("BalamceStatus") == "SHADOW", F.col("SeqNum")
                    ).otherwise(F.lit(None).cast("string")))
        .withColumn("LDGR_BAL_AMT",
                    F.when(
                        F.col("BalamceStatus") == "POSTED", F.col("Balance")
                    ).otherwise(F.lit(None).cast("decimal(23,4)")))
        .withColumn("LDGR_BAL_DTTM",
                    F.when(
                        F.col("BalamceStatus") == "POSTED", F.col("Timestamp")
                    ).otherwise(F.lit(None).cast("timestamp")))
        .withColumn("LDGR_BAL_SEQ",
                    F.when(
                        F.col("BalamceStatus") == "POSTED", F.col("SeqNum")
                    ).otherwise(F.lit(None).cast("string")))
    ).select(target_columns)

    return final_df

def transform_data(
        ctx: ApplicationContext,
        source_df: DataFrame,
) -> DataFrame:
    logger.info("SPARK_SESSION_OBJECT = %s", ctx.spark)
    config = ctx.config
    spark = ctx.spark
    params = config.transform_params
    task_id = config.pipeline_task_id

    logger.info(f'executing transformation: task id={task_id}, params={params}')

    transform_cfg = read_transform_config(params, task_id)

    # if transform_cfg.source_data_query.audit_columns:
    #     audit_df = read_audit_data(spark, config, transform_cfg.source_data_query)
    #     audit_df = audit_df.persist()

    reference_df = prepare_reference_data(
        spark, config, transform_cfg.ref_data_query
    )
    source_df.show()
    reference_df.show()
    
    transformed_df = transform_source_data(
        spark, transform_cfg.source_data_query, reference_df
    )
    transformed_df.printSchema()
    logger.info(ctx.spark.catalog.listTables())
    sys.exit()
    transformed_df = transform_column_mapping(
        config, transform_cfg.mapping, transformed_df
    )

    # if transform_cfg.source_data_query.audit_column:
    #     transformed_df = transform_audit_columns(
    #         transform_cfg.source_data_query, transformed_df, audit_df
    #     )

    transformed_df = transformed_df.drop_duplicates().persist()
    logger.info('number of rows after transformations: {}'.format(transformed_df.count()))

    return transformed_df

def read_transform_config(
        params: TransformParams, task_id: str
) -> TransformConfig:
    
    with open(params.reference_data) as f:
        ref_data_cfg = read_reference_data_config(f, task_id, params.reference_schema_mapping)

    with open(params.source_data) as f:
        src_data_cfg = read_source_data_config(f, task_id)
        logger.info("SOURCE_DATA_CFG = %s",src_data_cfg)

    with open(params.mapping) as f:
        #mapping_cfg = read_mapping_config(f, task_id)
        mapping_cfg = {}

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

    logger.info(
        f'transformation mappings read:',
        f'pipeline task = {task_id}, count = {len(rules)}'
    )

    if len(rules) == 0:
        raise ValueError('Cannot find transformation column mapping configuration')

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
    )

    to_db_type = lambda v: DatabaseType[v.upper()]
    schema = str, int, str, str, to_db_type
    assert len(schema) == len(indexes)

    extract = itemgetter(*indexes)
    to_row = lambda item: [t(v) for t, v in zip(schema, extract(item))]

    items = (ReferenceDataQuery(*to_row(item)) for item in reader)
    rules = query_pipeline_sorted(items, task_id)
    logger.info("reference_schema_mapping = %s", reference_schema_mapping)
    if reference_schema_mapping:
        rules = [update_reference_schema_mapping(rule, reference_schema_mapping) for rule in rules]

    logger.info(
        f'transformation reference data read:'
        f' pipeline task={task_id}, count={len(rules)}'
    )
    return rules

def prepare_reference_data(
        spark: SparkSession,
        stream_config: StreamConfig,
        config: Sequence,
) -> tp.Optional[DataFrame]:
    df = None
    logger.info("stream_config = %s",stream_config)
    logger.info("stream_config.phoenix_data_source = %s",stream_config.phoenix_data_source)
    #logger.info("stream_config.phoenix_data_source.zkurl = %s",stream_config.phoenix_data_source.zkurl)
    
    for rq in config:
        if rq.db_type == DatabaseType.PHEONIX:
            df = read_pheonix_table(spark, rq.query, ['*'], "jdbc:mysql://localhost:3306/ods")
        elif rq.db_type == DatabaseType.HIVE:
            df = spark.sql(rq.query)
            df = df.persist()
        elif rq.db_type == DatabaseType.SPARKSQL:
            df = spark.sql(rq.query)
        else:
            raise ValueError('Unknown database type: {}'.format(rq.db_type))
        
        df.createTempView(rq.table)
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
    elif mapping.mapping == MappingType.BATCH_PROCESS_NAME:
        col = F.lit(config.batch_process_name)
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
