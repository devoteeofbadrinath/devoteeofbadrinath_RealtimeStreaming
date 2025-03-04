import csv
import typing as tp
from collections.abc import Sequence, Iterable
import logging
from enum import auto, Enum
from operator import itemgetter, attrgetter
from dataclasses import dataclass
import dataclasses as dtc

from pyspark.sql.types import *
from pyspark.sql import SparkSession, Column, DataFrame, functions as F

sparkSession = SparkSession.builder.appName('StreamingStaticJoin').getOrCreate()

sparkSession.sparkContext.setLogLevel('ERROR')



logger = logging.getLogger(__name__)

static_schema = StructType([StructField('name', StringType(), True),
                         StructField('year', StringType(), True),
                         StructField('director', StringType(), True),
                         StructField('writer', StringType(), True),
                         StructField('star', StringType(), True)]
                         )
    
static_data = sparkSession.read\
                        .format("csv")\
                        .option("header", "true")\
                        .schema(static_schema)\
                        .load('/Users/shivammittal/Desktop/Deloitte/BOI_ODS_Python/DemoTest/source_movies.csv')
                        
@dataclass(frozen=True)
class ApplicationContext:
    #config: BatchConfig
    spark: SparkSession
    #generic_args: dict
    #job_args: dict
    #dag_args: dict

ctx = ApplicationContext(sparkSession)

class JoinType(Enum):
    LEFT = "left"
    RIGHT = "right"
    UNION = "union"
    NONE = "none"

@dataclass(frozen=True)
class SourceDataQuery:
    pipeline_task_id: str
    query: str
    primary_key: str
    join_type: JoinType
    merge_key: str
    audit_column: str

class DatabaseType(Enum):
    HIVE = "hive"
    PHEONIX = "pheonix"
    S3 = "s3"

@dataclass(frozen=True)
class ReferenceDataQuery:
    pipeline_task_id: str
    order: int
    query: str
    table: str
    db_type: DatabaseType

@dataclass(frozen=True)
class TransformParams:
    reference_data: str
    source_data: str
    mapping: str
    reference_schema_mapping: dict

@dataclass(frozen=True)
class TransformConfig:
    ref_data_query: Sequence
    source_data_query: SourceDataQuery
    mapping: Sequence

@dataclass(frozen=True)
class TransformConfig:
    ref_data_query: Sequence
    source_data_query: SourceDataQuery
    #mapping: Sequence

def transform_data(
        ctx: ApplicationContext,
        source_df: DataFrame,
) -> DataFrame:
    #config = ctx.config
    spark = ctx.spark
    print('spark = ', spark)
    print('type(spark) = ', type(spark))
    #params = config.transform_params
    #task_id = config.pipeline_task_id

    #logger.info(f'executing transformation: task id={task_id}, params={params}')

    #transform_cfg = read_transform_config(params, task_id)
    transform_cfg = read_transform_config()

    #if transform_cfg.source_data_query.audit_columns:
    #    audit_df = read_audit_data(spark, config, transform_cfg.source_data_query)
    #    audit_df = audit_df.persist()

    source_df.createTempView('source_df')
    source_df.show()
    reference_df = prepare_reference_data(
        spark, 
        #config, 
        transform_cfg.ref_data_query
    )
    reference_df.show()

    transformed_df = transform_source_data(
        spark, transform_cfg.source_data_query, reference_df
    )

    #transformed_df = transform_column_mapping(
    #    config, transform_cfg.mapping, transformed_df
    #)

    #if transform_cfg.source_data_query.audit_column:
    #    transformed_df = transform_audit_columns(
    #        transform_cfg.source_data_query, transformed_df, audit_df
    #    )

    #transformed_df = transformed_df.drop_duplicates().persist()
    #logger.info('number of rows after transformations: {}'.format(transformed_df.count()))

    #return transformed_df
    transformed_df.show()
    return transformed_df

def read_transform_config(
        #params: TransformParams, task_id: str
) -> TransformConfig:
    
    #with open(params.reference_data) as f:
    #    ref_data_cfg = read_reference_data_config(f, task_id, params.reference_schema_mapping)

    #with open(params.source_data) as f:
    #    src_data_cfg = read_source_data_config(f, task_id)

    with open("/Users/shivammittal/Desktop/Deloitte/BOI_ODS_Python/cdpods_data_processing/config/data/transformation_ref_data.csv") as f:
        ref_data_cfg = read_reference_data_config(f, "account_balance_pipeline_name_pipeline_task_name", {"ods_refd":"ods_refd_test"})

    with open("/Users/shivammittal/Desktop/Deloitte/BOI_ODS_Python/cdpods_data_processing/config/data/source_data_query.csv") as f:
        src_data_cfg = read_source_data_config(f, "account_balance_pipeline_name_pipeline_task_name")
    
    print('ref_data_cfg = ',ref_data_cfg)
    print(type(ref_data_cfg))

    print('src_data_cfg = ',src_data_cfg)
    print(type(src_data_cfg))

    #with open(params.mapping) as f:
    #    mapping_cfg = read_mapping_config(f, task_id)

    return TransformConfig(ref_data_cfg, src_data_cfg)
    #return TransformConfig(ref_data_cfg, src_data_cfg, mapping_cfg)

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

def query_pipeline(data: Iterable, task_id: str) -> Sequence:
    return [item for item in data if item.pipeline_task_id == task_id]

def query_pipeline_sorted(data: Iterable, task_id: str) -> Sequence:
    items = query_pipeline(data, task_id)
    return sorted(items, key = attrgetter('order'))

def csv_column_index(header: list, column:str) -> int:

    logger.debug('header: {}'.format(header))
    try:
        return header.index(column)
    except ValueError as ex:
        raise ValueError(f'Column not found: {column}') from ex
    
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

    print('indexes = ', indexes)

    to_db_type = lambda v: DatabaseType[v.upper()]
    print('to_db_type = ', to_db_type)

    schema = str, int, str, str, to_db_type
    print('schema = ', schema)

    assert len(schema) == len(indexes)

    extract = itemgetter(*indexes)
    print('extract = ', extract)

    to_row = lambda item: [t(v) for t, v in zip(schema, extract(item))]

    items = (ReferenceDataQuery(*to_row(item)) for item in reader)
    rules = query_pipeline_sorted(items, task_id)
    print('rules 1 = ', rules)

    rules = [update_reference_schema_mapping(rule, reference_schema_mapping) for rule in rules]
    print('rules 2 = ', rules)

    logger.info(
        f'transformation reference data read:'
        f' pipeline task={task_id}, count={len(rules)}'
    )
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


def prepare_reference_data(
        spark: SparkSession,
        #batch_config: BatchConfig,
        config: Sequence,
) -> tp.Optional[DataFrame]:
    df = None
    print('prepare_reference_data_spark = ', spark)
    print('prepare_reference_data_spark_type(spark) = ', type(spark))
    
    for rq in config:
        print('rq = ',rq)
        print('rq.db_type = ',rq.db_type)
        if rq.db_type == DatabaseType.PHEONIX:
            pass
            #df = read_pheonix_table(spark, rq.query, ['*'], batch_config.data_output.zkurl)
        elif rq.db_type == DatabaseType.HIVE:
            df = spark.sql(rq.query)
            df = df.persist()
        elif rq.db_type == DatabaseType.S3:
            df = spark.read\
                        .format("csv")\
                        .option("header", "true")\
                        .schema(static_schema)\
                        .load('/Users/shivammittal/Desktop/Deloitte/BOI_ODS_Python/DemoTest/reference_movies.csv')


        else:
            raise ValueError('Unknown database type: {}'.format(rq.db_type))
        
        print('df = ',df)
        print('type(df) =',type(df))
        df.createTempView(rq.table)
    return df

def transform_source_data(
        spark: SparkSession,
        config: SourceDataQuery,
        reference_df: tp.Optional[DataFrame]
) -> DataFrame:
    
    if config.join_type != JoinType.NONE and reference_df is None:
        raise ValueError('Join type specified, but no reference data')
    
    df = spark.sql(config.query)
    if config.join_type != JoinType.NONE and reference_df is not None:
        df = df.join(
            reference_df, on=config.merge_key, how=config.join_type.value
        )
    return df


#with open("/Users/shivammittal/Desktop/Deloitte/BOI_ODS_Python/cdpods_data_processing/config/data/transformation_ref_data.csv") as f:
#    ref_data_cfg = read_reference_data_config(f, "af_edw_entparty_ods_batch_landing_ods_load", {"ods_refd":"ods_refd_test"})

#with open("/Users/shivammittal/Desktop/Deloitte/BOI_ODS_Python/cdpods_data_processing/config/data/source_data_query.csv") as f:
#    src_data_cfg = read_source_data_config(f, "account_balance_pipeline_name_pipeline_task_name")

#print('ref_data_cfg = ',ref_data_cfg)
#print(type(ref_data_cfg))

#print('src_data_cfg = ',src_data_cfg)
#print(type(src_data_cfg))

transformed_df = transform_data(ctx, static_data)
