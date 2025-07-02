import io
import logging
import typing as tp
from datetime import datetime, date, timezone
from functools import partial
from pyspark.sql import SparkSession

from brdj_stream_processing.stream import transform_module as tm
from brdj_stream_processing.stream import stream_types as st
from brdj_stream_processing.stream.stream_types import TransformParams

import pytest
from unittest import mock
from tests.conftest import df_column


logger = logging.getLogger(__name__)

SOURCE_DATA_CONFIG_HEADER = """\
pipeline_task_id,pipeline_name,pipeline_task_name,sql_query,primary_key,join_type,merge_key,sort_key,audit_column,version,last_updated,last_updated_by
"""

SOURCE_DATA_CONFIG = SOURCE_DATA_CONFIG_HEADER + """\
p1_t1,p1,t1,"select * from source_1 where col_1 = 'a'",p_key_a,right,col_a,update_date,an_audit_col_a,1,2024-11-04,c138147
p2_t2,p2,t2,"select * from source_2",p_key_b,union,col_b,update_date,an_audit_col_b,1,2024-11-04,c138147
"""

# NOTE: queries not in order to test ordering
REFERENCE_DATA_CONFIG = """\
pipeline_task_id,pipeline_name,pipeline_task_name,query_order,transformation_step,database_type,sql_query,temp_table_name,read_in_batches,version,last_updated,last_updated_by
p1_t1,p1,t1,2,transform,phoenix,schema.table,tmp_ref2,,1,2024--11-04,c138147
p1_t1,p1,t1,3,transform,phoenix,schema.table,tmp_ref3,,1,2024--11-04,c138147
p1_t1,p1,t1,1,transform,phoenix,schema.table,tmp_ref1,,1,2024--11-04,c138147
p2_t2,p2,t2,2,transform,phoenix,schema.table,tmp_ref2,,1,2024--11-04,c138147
p2_t2,p2,t2,1,transform,phoenix,schema.table,tmp_ref1,,1,2024--11-04,c138147
p3_t3,p3,t3,2,normalise,phoenix,schema.table,tmp_ref2,,1,2024--11-04,c138147
p3_t3,p3,t3,1,normalise,phoenix,schema.table,tmp_ref1,,1,2024--11-04,c138147
p4_t4,p4,t4,1,transform,phoenix,"Select * from input",tmp_ref1,,1,2024--11-04,c138147
p5_t5,p5,t5,1,transform,sparksql,"Select * from input",tmp_ref1,,1,2024--11-04,c138147
"""

# NOTE: mapping not in order to test ordering
MAPPING_CONFIG = """\
pipeline_task_id,pipeline_name,pipeline_task_name,source_col_name,target_col_name,column_order,target_data_type,transformation_rule,version,last_updated,last_updated_by
p1_t1,p1,t1,city,city_renamed,2,string,city || ' ' || city,1,2025-03-05,c138147
p1_t1,p1,t1,country_code,cc,1,string,passthrough,1,2025-03-05,c138147
p1_t1,p1,t1,population,num_of_people_min,4,integer,population / 1000000,1,2025-03-05,c138147
p1_t1,p1,t1,country,a_country,3,string,passthrough,1,2025-03-05,c138147
p1_t1,p1,t1,last_updated,timestamp,5,when_done,current_timestamp(),1,2025-03-05,c138147
p2_t2,p2,t2,id,integer,1,id,passthrough,1,2025-03-05,c138147
p3_t3,p3,t3,id,integer,1,id,passthrough,1,2025-03-05,c138147
"""


@pytest.fixture
def params() -> TransformParams:
    transform_params = TransformParams(
    reference_data='/app/mount/config/data/transformation_ref_data.csv',
    source_data='/app/mound/config/data/source_data_query.csv',
    mapping='/app/mount/config/data/transformation_mapping.csv',
    reference_schema_mapping={"brdj":"test_brdj","brdj_refd":"test_brdj_refd","brdj_stg":"test_brdj_stg"}
    )

    return transform_params


def test_read_source_data_config() -> None:
    """
    Test parsing source data query configuration.
    """
    f = io.StringIO(SOURCE_DATA_CONFIG)
    config = tm.read_source_data_config(f, 'p1_t1')

    assert config.pipeline_task_id == 'p1_t1'
    assert config.query == 'select * from source_1 where col_1 = \'a\''
    assert config.primary_key == 'p_key_a'
    assert config.join_type == st.JoinType.RIGHT
    assert config.merge_key == 'col_a'
    assert config.audit_column == 'an_audit_col_a'


def test_read_source_data_config_empty() -> None:
    """
    Test if error is raised when no source data query configuration is
    found.
    """
    f = io.StringIO(SOURCE_DATA_CONFIG_HEADER) # just use header as input
    with pytest.raises(ValueError) as ctx_ex:
        tm.read_source_data_config(f, 'p1_t1')
    assert str(ctx_ex.value) == 'Cannot find source data query configuration'


def test_read_source_data_config_many() -> None:
    """
    Test if error is related when multiple source data query configuration 
    entries are found.
    """
    # duplicate (p1, t1)
    data = SOURCE_DATA_CONFIG + 'p1_t1,p1,t1,q,p,left,m,s,a,v,l,l'
    f = io.StringIO(data)
    with pytest.raises(ValueError) as ctx_ex:
        tm.read_source_data_config(f, 'p1_t1')
    assert str(ctx_ex.value) == 'Multiple source data query configuration entries'


@pytest.mark.parametrize(
    'task_id, expected_orders, expected_query, expected_db_type, expected_step',
    [
        ('p1_t1', [1, 2, 3], 'schema.table', st.DatabaseType.PHOENIX, st.TransformationStep.TRANSFORM),
        ('p2_t2', [1, 2], 'schema.table', st.DatabaseType.PHOENIX, st.TransformationStep.TRANSFORM),
        ('p3_t3', [1, 2], 'schema.table', st.DatabaseType.PHOENIX, st.TransformationStep.NORMALISE),
        ('p5_t5', [1], 'Select * from input', st.DatabaseType.SPARKSQL, st.TransformationStep.TRANSFORM),
    ]
)
def test_Read_Reference_data_config(
    params: TransformParams,
    task_id: str,
    expected_orders: list[int],
    expected_query: str,
    expected_db_type: st.DatabaseType,
    expected_step: st.TransformationStep,
) -> None:
    """
    Test parsing reference data configuration with different task IDs and expectations.
    """
    f = io.StringIO(REFERENCE_DATA_CONFIG)
    rules = tm.read_reference_data_config(f, task_id, params.reference_schema_mapping)

    print("rules = %s", rules)
    print("expected = %s", expected_orders)

    assert len(rules) == len(expected_orders)
    for i, rq in zip(expected_orders, rules):
        logger.info('config={}, expected={}'.format(rq, i))

        assert rq.pipeline_task_id == task_id
        assert rq.order == i
        assert rq.query == expected_query
        assert rq.table == f'tmp_ref{i}'
        assert rq.db_type == expected_db_type
        assert rq.transformation_step == expected_step
        
@pytest.mark.parametrize(
    'task_id, expected',
    [('p1_t1', [1, 2, 3]),
     ('p2_t2', [1,2])]
)
def test_Read_Reference_data_config_transform(
    params: TransformParams,
    task_id: str,
    expected: tuple[str, str, list[int]],
) -> None:
    """
    Test parsing reference data configuration.
    """
    f = io.StringIO(REFERENCE_DATA_CONFIG)
    rules = tm.read_reference_data_config(f, task_id, params.reference_schema_mapping)
    print("rules = %s", rules)
    print("expected = %s", expected)
    assert len(rules) == len(expected)
    for i, rq in zip(expected, rules):
        logger.info('config={}, expected={}'.format(rq,i))

        assert rq.pipeline_task_id == task_id
        assert rq.order == i
        assert rq.query == 'schema.table'
        assert rq.table == 'tmp_ref{}'.format(i)
        assert rq.db_type == st.DatabaseType.PHOENIX
        assert rq.transformation_step == st.TransformationStep.TRANSFORM

@pytest.mark.parametrize(
    'task_id, expected',
    [('p3_t3', [1, 2])]
)
def test_Read_Reference_data_config_normalise(
    params: TransformParams,
    task_id: str,
    expected: tuple[str, str, list[int]],
) -> None:
    """
    Test parsing reference data configuration.
    """
    f = io.StringIO(REFERENCE_DATA_CONFIG)
    rules = tm.read_reference_data_config(f, task_id, params.reference_schema_mapping)
    print("rules = %s", rules)
    print("expected = %s", expected)
    assert len(rules) == len(expected)
    for i, rq in zip(expected, rules):
        logger.info('config={}, expected={}'.format(rq,i))

        assert rq.pipeline_task_id == task_id
        assert rq.order == i
        assert rq.query == 'schema.table'
        assert rq.table == 'tmp_ref{}'.format(i)
        assert rq.db_type == st.DatabaseType.PHOENIX
        assert rq.transformation_step == st.TransformationStep.NORMALISE

@pytest.mark.parametrize(
    'task_id, expected',
    [('p5_t5', [1])]
)
def test_Read_Reference_data_config_sparksql(
    params: TransformParams,
    task_id: str,
    expected: tuple[str, str, list[int]],
) -> None:
    """
    Test parsing reference data configuration.
    """
    f = io.StringIO(REFERENCE_DATA_CONFIG)
    rules = tm.read_reference_data_config(f, task_id, params.reference_schema_mapping)
    print("rules = %s", rules)
    print("expected = %s", expected)
    assert len(rules) == len(expected)
    for i, rq in zip(expected, rules):
        logger.info('config={}, expected={}'.format(rq,i))

        assert rq.pipeline_task_id == task_id
        assert rq.order == i
        assert rq.query == 'Select * from input'
        assert rq.table == 'tmp_ref{}'.format(i)
        assert rq.db_type == st.DatabaseType.SPARKSQL
        assert rq.transformation_step == st.TransformationStep.TRANSFORM

@pytest.mark.parametrize(
    'task_id, expected',
    [('p4_t4', [1])]
)
def test_incorrect_schema_table_reference_mapping(
    params: TransformParams,
    task_id: str,
    expected: tuple[str, str, list[int]],
) -> None:
    """
    Test if error is raised when invalid reference table format is found.
    """
    f = io.StringIO(REFERENCE_DATA_CONFIG) # just use header as input
    with pytest.raises(ValueError) as ctx_ex:
        tm.read_reference_data_config(f, task_id, {"BRDJ":"brdj"})
    assert str(ctx_ex.value) == 'Invalid reference table format: Select * from input. Expected <schema>.<table>.'

def test_read_mapping_config() -> None:
    """
    Test parsing transformation column mapping config.
    """
    f = io.StringIO(MAPPING_CONFIG)
    config = tm.read_mapping_config(f, 'p1_t1')

    assert len(config) == 5

    pipeline = {m.pipeline_task_id for m in config}
    assert pipeline == {('p1_t1')}

    assert [m.order for m in config] == list(range(1, 6))

    m1, m2, m3, m4, m5 = config

    assert m1.source == 'country_code'
    assert m1.target == 'cc'
    assert m1.target_type == 'string'
    assert m1.mapping == st.MappingType.PASSTHROUGH

    assert m2.source == 'city'
    assert m2.target == 'city_renamed'
    assert m2.target_type == 'string'
    assert m2.mapping == "city || ' ' || city"

    assert m3.source == 'country'
    assert m3.target == 'a_country'
    assert m3.target_type == 'string'
    assert m3.mapping == st.MappingType.PASSTHROUGH

    assert m4.source == 'population'
    assert m4.target == 'num_of_people_min'
    assert m4.target_type == 'integer'
    assert m4.mapping == 'population / 1000000'

    assert m5.source == 'last_updated'
    assert m5.target == 'timestamp'
    assert m5.target_type == 'when_done'
    assert m5.mapping == 'current_timestamp()'


def test_read_mapping_config_empty() -> None:
    """
    Test if error is raised when no transformation column mapping config is
    found.
    """
    f = io.StringIO(MAPPING_CONFIG)
    with pytest.raises(ValueError) as ctx_ex:
        tm.read_mapping_config(f, 'not-exists')
    assert str(ctx_ex.value) == 'Cannot find transformation column mapping configuration'


@pytest.mark.parametrize(
    'transformation_step',
    [(st.TransformationStep.TRANSFORM),
     (st.TransformationStep.NORMALISE)]
)
def test_prepare_reference_data(
        spark_session: SparkSession, stream_config: st.StreamConfig, transformation_step: st.TransformationStep.TRANSFORM
) -> None:
    """
    Test execution og SQL queries when preparing reference data.
    
    Reference data configuration for this test consists of two queries
    reading data from `country` and `city` tables. We want to join the two
    tables on country code and have final table with city name and country
    name.
    """
    query1 = 'select code, name from country'
    query2 = """
select city, r1.name as country
from tmp_ref1 r1 inner join city on r1.code = city.country_code
    """
    create_df = spark_session.createDataFrame
    config = [
        st.ReferenceDataQuery(
            'p1_t1', 1, query1, 'tmp_ref1', st.DatabaseType.HIVE, transformation_step
        ),
        st.ReferenceDataQuery(
            'p1_t1', 2, query2, 'country_city', st.DatabaseType.HIVE, transformation_step
        ),
    ]

    # NOTE: the result table country_city shall have no `col1` column
    columns = ['code', 'name', 'col1']
    ref1_data = [
        ('JP', 'Japan', 'col1a'),
        ('IE', 'Ireland', 'col1b'),
    ]
    df = create_df(ref1_data, schema=columns)
    df.createTempView('country')

    #Note the result table country_city shall have no `col2` column
    columns = ['country_code', 'city', 'col2']
    ref2_data = [
        ('JP', 'Tokyo', 'col2a'),
        ('JP', 'Kyoto', 'col2b'),
        ('IE', 'Dublin', 'col2c'),
        ('IE', 'Cork', 'col2d'),
    ]
    df = create_df(ref2_data, schema=columns)
    df.createTempView('city')

    tm.prepare_reference_data(spark_session, stream_config, config, transformation_step)

    df = spark_session.table('country_city')
    result = [list(row) for row in df.orderBy('city').collect()]

    expected = [
        ['Cork', 'Ireland'],
        ['Dublin', 'Ireland'],
        ['Kyoto', 'Japan'],
        ['Tokyo', 'Japan'],
    ]
    assert df.schema.fieldNames() == ['city', 'country']
    assert result == expected


def test_prepare_reference_empty(
        spark_session: SparkSession, stream_config: st.StreamConfig
) -> None:
    """
    Test preparing reference data for empt config.
    
    When there is no reference data configuration, then null is returned.
    """
    result = tm.prepare_reference_data(spark_session, stream_config, [], st.TransformationStep.NORMALISE)
    result is None


def test_transform_source_data(spark_session: SparkSession) -> None:
    """
    Test transformation with source data query.
    """
    create_df = spark_session.createDataFrame
    query = 'select * from source_data'
    config = tm.SourceDataQuery(
        'p1_t1', query, 'city', st.JoinType.LEFT, 'code', 'start_date'
    )

    ref_data = [
        ('JP', 'Japan'),
        ('IE', 'Ireland')
    ]
    ref_df = create_df(ref_data, schema=['code', 'country'])

    now = date(2024, 1, 1)
    columns = ['code', 'city', 'population', 'audit.start_date']
    source_data = [
        ('JP', 'Kyoto', 300, now),
        ('JP', 'Tokyo', 400, now),
        ('IE', 'Dublin', 500, now),
        ('IE', 'Cork', 600, now),
    ]
    df = create_df(source_data, schema=columns)
    df.createTempView('source_data')

    result = tm.transform_source_data(spark_session, config, ref_df)
    result = result.orderBy('code', 'city')
    col_result = partial(df_column, result)

    assert col_result('code') == ['IE', 'IE', 'JP', 'JP']
    assert col_result('country') == ['Ireland', 'Ireland', 'Japan', 'Japan']
    assert col_result('city') == ['Cork', 'Dublin', 'Kyoto', 'Tokyo']
    assert col_result('population') == [600, 500, 300, 400]
    assert col_result('`audit.start_date`') == [now] * 4


def test_transform_source_data_join_none(spark_session: SparkSession) -> None:
    """
    Test transformation with source data query, but it's join set to none.
    """
    create_df = spark_session.createDataFrame
    query = """\
select s.*, r.country
from source_data s left join ref_data r on s.code = r.code
    """
    # Join is done via the source data query, so join type is set to none
    config = tm.SourceDataQuery(
        'p1_t1', query, 'city', st.JoinType.NONE, '', 'audit.start_date'
    )

    ref_data = [
        ('JP', 'Japan'),
        ('IE', 'Ireland'),
    ]
    ref_df = create_df(ref_data, schema=['code', 'country'])
    ref_df.createTempView('ref_data')

    ref_data_unused = [
        ('JP', 'Japan x'),     # not used due to join type set to none
        ('IE', 'Ireland x'),   # not used due to join type set to none
    ]
    ref_unused_df = create_df(ref_data_unused, schema=['code', 'country'])

    now = date(2024, 1, 1)
    columns = ['code', 'city', 'population', 'start_date']
    source_data = [
        ('JP', 'Kyoto', 300, now),
        ('JP', 'Tokyo', 400, now),
        ('IE', 'Dublin', 500, now),
        ('IE', 'Cork', 600, now),
    ]
    df = create_df(source_data, schema=columns)
    df.createTempView('source_data')

    result = tm.transform_source_data(spark_session, config, ref_unused_df)
    result = result.orderBy('code', 'city')
    col_result = partial(df_column, result)

    assert col_result('code') == ['IE', 'IE', 'JP', 'JP']
    assert col_result('country') == ['Ireland', 'Ireland', 'Japan', 'Japan']
    assert col_result('city') == ['Cork', 'Dublin', 'Kyoto', 'Tokyo']
    assert col_result('population') == [600, 500, 300, 400]
    assert col_result('start_date') == [now] * 4
    

def test_transform_source_date_join_error() -> None:
    """
    Test if error is raised when reference join type is specified, but no
    reference data exists.
    """
    spark = mock.MagicMock()
    query = 'select ...'
    config = tm.SourceDataQuery('p1_t1', query, 'city', st.JoinType.LEFT, '', '')

    with pytest.raises(ValueError) as ex_ctx:
        tm.transform_source_data(spark, config, None)

    assert str(ex_ctx.value) == 'Join type specified, but no reference data'


def test_transform_column_mapping(
        spark_session: SparkSession, stream_config: st.StreamConfig
) -> None:
    """
    Test applying column mappings during transformation.
    """
    create_df = spark_session.createDataFrame

    col_mapping = [
        st.ColumnMapping(
            'p1_t1', 1, 'code', 'code', "string",
            "code || '_' || code"
        ),
        st.ColumnMapping(
            'p1_t1', 2, 'city', 'city_name', "string",
            st.MappingType.PASSTHROUGH
        ),
        st.ColumnMapping(
            'p1_t1', 3, 'population', 'num_min', "integer",
            'round(population / 1000000)'
        ),
        st.ColumnMapping(
            'p1_t1', 4, 'last_update', 'when_done', "timestamp",
            'current_timestamp()'
        ),
    ]
    columns = ['code', 'city', 'population', 'last_update_date']
    data = [
        ['JP', 'Kyoto', 11001023, '2024-01-05 10:00:00'],
        ['JP', 'Tokyo', 12001023, '2024-01-09 10:00:00'],
        ['IE', 'Dublin', 13001023, '2024-01-02 10:00:00'],
        ['IE', 'Cork', 14001023, '2024-01-03 10:00:00'],
    ]
    df = create_df(data, schema=columns)

    # record time when we start running the test, so we can verify
    # timestamp column(`when_done`)
    start = datetime.now(tz=timezone.utc)
    df_result = tm.transform_column_mapping(stream_config, col_mapping, df) \
        .orderBy('code', 'city')
    
    col_result = partial(df_column, df_result)
    assert col_result('code') == ['IE_IE', 'IE_IE', 'JP_JP', 'JP_JP']
    assert col_result('city_name') == ['Cork', 'Dublin', 'Kyoto', 'Tokyo']

    population = col_result('num_min')
    assert population == [14, 13, 11, 12]
    assert all(type(v) is int for v in population)

    ts_col = col_result('when_done')
    # all columns have exactly the same timestamp
    assert len(set(ts_col)) == 1

    # timestamp is updated with current time
    ts = ts_col[0].astimezone(timezone.utc)
    assert start <= ts <= datetime.now(tz=timezone.utc)


@pytest.mark.parametrize(
    'value, target_type, mapping, expected',
    [('7', 'integer', st.MappingType.PASSTHROUGH, 7),
     (13, 'string', st.MappingType.PASSTHROUGH, '13'),
     
     # simple cast to date, input is ISO formatted string
     ('2024-01-25', 'date', st.MappingType.PASSTHROUGH, date(2024, 1, 25)),

     # stream process name
     ('any', 'string', st.MappingType.STREAM_PROCESS_NAME, 'brdj-dev-stream-process'),
     
     # conversion to date, input is string in format DD/MM/YY
     ('25/01/24', 'date', 'to_date(source, \'dd/MM/yy\')', date(2024, 1, 25))]
)
def test_column_mapping(
    spark_session: SparkSession,
    stream_config: st.StreamConfig,
    value: tp.Any | None,
    target_type: str,
    mapping: st.MappingType | str,
    expected: tp.Any,
) -> None:
    """
    Test columnmapping with column mapping configuration.
    """
    df = spark_session.createDataFrame([[value]], schema=['source'])

    m = st.ColumnMapping(
        'p1_t1', 0, 'source', 'target', target_type, mapping
    )

    df = df.withColumn('target', tm.map_column(stream_config, m))
    assert df_column(df, 'target') == [expected]


