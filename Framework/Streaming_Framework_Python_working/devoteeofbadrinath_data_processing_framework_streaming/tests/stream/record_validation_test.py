"""
Unit tests for validation of recrds
"""

from __future__ import annotations

import io
from functools import partial
from unittest import mock
import pytest
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, Row
from brdj_stream_processing.stream.record_validation_module import \
    is_enum, is_mandatory, is_date, is_integer, is_float, \
        parse_validation_config, _validate_records_meta, validate_records
from brdj_stream_processing.stream.stream_types import ValidationType, ValidationName, \
    ValidationRecordConfig, ValidationRecordColumn, PipelineData, ApplicationContext
from ..conftest import df_column


VALIDATION_CONFIG = """\
pipeline_task_id,pipeline_name,col_name,validation_type,source_date_format,enum_value,validation_name
p_src,af_edw_srcparty,src_prty_ident,both,,,validation_key
p_ent,af_edw_entparty,entrp_prty_ident,pre_transform,,,validation_key
p_src,af_edw_srcparty,record_deleted_flag,pre_transform,,"0,1",is_enum
p_ent,af_edw_entparty,record_deleted_flag,pre_transform,,"0,1",is_enum
p_ent,af_edw_entparty,data_src_cd,post_transform,,,is_mandatory
p_src,af_edw_srcparty,data_src_cd,both,,,is_mandatory
p_ent,af_edw_entparty,data_src_qlfr,post_transform,,,is_mandatory
p_src,af_edw_srcparty,data_src_qlfr,post_transform,,,is_mandatory
p_src,af_edw_srcparty,dcsd_dt,post_transform,YY/MM/DD,,is_valid_date
p_ent,af_edw_entparty,dcsd_dt,post_transform,YYYY-MM--DD,,is_valid_date
p_ent,af_edw_entparty,entrp_prty_ident,pre_transform,,,is_mandatory
p_src,af_edw_srcparty,entrp_prty_ident,pre_transform,,"Y,N,",is_enum
p_src,af_edw_srcparty,src_prty_ident_desc_cd,pre_transform,,,is_mandatory
"""

SUMMARY_COLUMNS = [
    'source_file_name',
    'pipeline_name',
    'field_validated',
    'record_id_key',
    'record_id_value',
    'validated_data',
    'validated_type',
    'validated_name',
    'failure_reason',
    'validation_timestamp',
    'validation_yr_month',
]

# create row with metadata for validation of records
meta_row = Row('validation', 'valid')


@pytest.mark.parametrize(
    'func, data, expected',
    [[is_integer,
    [(1, 'abc'),
    (2, '2'),
    (3, '3'),
    (4, '0'),
    (5, '21.5'),
    (6, None),
    (7, '!'),
    (8, '$'),
    (9, '+12'),
    (10, '-12')],
    [False, True, True, True, False, True, False, False, True, True]],
    
    [is_float,
    [(1, 'abc'),
    (2, '2'),
    (4, '0'),
    (5, '21.5'),
    (6, None),
    (7, '-1.0'),
    (8, '+2.0')],
    [False, False, False, True, True, True, True]],
    
    #enum values have *no* null values
    [partial(is_enum, values=['0','1']),
    [(1, 'a'),
    (2, '0'),
    (3, '2'),
    (4, '1'),
    (5, '2'),
    (6, '1'),
    (7, None),
    (8, '0')],
    [False, True, False, True, False, True, False, True]],
    
    # enum values have a null value
    [partial(is_enum, values=['0', '1', None]),
    [(1, 'a'),
    (2, '0'),
    (3, '2'),
    (4, '1'),
    (5, '2'),
    (6, '1'),
    (7, None),
    (8, '0')],
    [False, True, False, True, False, True, True, True]],
    
    [is_mandatory,
    [(1, 'a'),
    (2, None),
    (3, 'b'),
    (4, None)],
    [True, False, True, False]],
    
    [partial(is_date, date_fmt='dd/MM/yyyy'),
    [(1, '06/13/2024'),
    (2, '31/06/2024'),
    (3, '07/06/2024'),
    (4, '24/09/2024'),
    (5, 'a-string'),
    (6, None)],
    [False, False, True, True, False, True]]])
def test_validator(
    spark_session: SparkSession,
    func,
    data: list, # list[tuple[int, str | None]]
    expected: list, # list[bool],
) -> None:
    """
    Test record validator function like 'is_integer', 'is_date', etc.
    """
    df = spark_session.createDataFrame(data, ['id', 'value'])
    df = df.select(func('value').alias('is_valid'))
    assert df_column(df, 'is_valid') == expected


def test_validate_records_rejected(ctx: ApplicationContext) -> None:
    """
    Test validation of records with accumulation of rejected rows.

    Run validation of records twice and check that validation error is
    raised due to accumulation of rejected rows
    """
    spark_session = ctx.spark

    schema = ['id', 'c1', 'c2']

    config = _validation_config(
        _validation_record('c1', ValidationName.IS_MANDATORY),
        _validation_record('c2', ValidationName.IS_ENUM, enum=['0', '1']),
    )

    # input data for pre-transform validation
    data_pre = [
        [1, 10, '0'],
        [2, 20, '1'],
        [3, None, '1'], # rejected count == 1
        [4, 30, '0'],
        [5, 40, '1'],
    ]
    df_pre = spark_session.createDataFrame(data_pre, schema)

    with mock.patch('brdj_stream_processing.stream.record_validation_module.read_validation_config') as mock_read_cfg, \
            mock.patch('brdj_stream_processing.stream.record_validation_module._write_rejected_records_to_dlq'):

        mock_read_cfg.return_value = config

        #rejected rows == 1 => 20% rejected => no exception
        ppl_data = PipelineData(df_pre)
        ppl_data = validate_records(ctx, ppl_data, ValidationType.PRE_TRANSFORM)
        #assert ppl_data.rejected == 1
        assert ppl_data.rejected == 0


def test_validate_records_meta_all_ok(spark_session: SparkSession) -> None:
    """
    Test validation of records when all input data is valid.
    """
    config = _validation_config(
        _validation_record('c1', ValidationName.IS_INTEGER),
        _validation_record('c2', ValidationName.IS_ENUM, enum=['0', '1']),
        _validation_record('c3', ValidationName.IS_MANDATORY),
    )

    # all data is valid
    data = [
        [10, '+2', '1', '123'],
        [20, '1', '0', 'abc'],
        [30, None, '0', 'xyz']
    ]
    schema = ['id', 'c1', 'c2', 'c3']
    to_row = Row(*schema)
    df = spark_session.createDataFrame(data, schema)
    
    #valid, rejected = _validate_records_meta(spark_session, config, df)
    valid = _validate_records_meta(spark_session, config, df)

    assert df_column(valid, 'id') == [10, 20, 30]
    assert df_column(valid, 'c1') == ['+2', '1', None]
    assert df_column(valid, 'c2') == ['1', '0', '0']
    assert df_column(valid, 'c3') == ['123', 'abc', 'xyz']
    expected = to_row(
        meta_row('is_integer', True),
        meta_row('is_enum', True),
        meta_row('is_mandatory', True),
    )
    assert df_column(valid, '_meta') == 3 * [expected]

    # no rejected rows as all data is valid
    # assert rejected.count() == 0


def test_validate_records_meta_many_entries(spark_session:SparkSession) -> None:
    """
    Test validation of records with multiple configuration entries for a
    column.
    """
    config = _validation_config(
        _validation_record('c1', ValidationName.IS_INTEGER), # c1 column has two entries
        _validation_record('c1', ValidationName.IS_MANDATORY),
        _validation_record('c2', ValidationName.IS_ENUM, enum=['0', '1']),
        _validation_record('c3', ValidationName.IS_MANDATORY)
    )

    # all data is valid
    data = [
        [10, '+2', '1', '123'],
        [20, '1', '0', 'abc'],
        [30, None, '0', 'xyz'],
    ]
    schema = ['id', 'c1', 'c2', 'c3']
    to_row = Row(*schema)
    df = spark_session.createDataFrame(data, schema)
    
    #valid, rejected = _validate_records_meta(spark_session. config, df)
    valid = _validate_records_meta(spark_session, config, df)
    valid = valid.filter(F.col("_meta_valid"))
    assert df_column(valid, 'id') == [10, 20]
    assert df_column(valid, 'c1') == ['+2', '1']
    assert df_column(valid, 'c2') == ['1', '0']
    assert df_column(valid, 'c3') == ['123', 'abc']
    expected = to_row(
        meta_row('is_integer', True),
        meta_row('is_mandatory', True),
        meta_row('is_enum', True),
        meta_row('is_mandatory', True),
    )
    assert df_column(valid, '_meta') == 2 * [expected]

    # assert df_column(rejected, 'id') == [30]
    # assert df_column(rejected, 'c1') == [None]
    # assert df_column(rejected, 'c2') == ['0']
    # assert df_column(rejected, 'c3') == ['xyz']
    # expected = to_row(
    #     meta_row('is_integer', True),
    #     meta_row('is_mandatory', False),
    #     meta_row('is_enum', True),
    #     meta_row('is_mandatory', True),
    # )
    # assert df_column(rejected, '_meta') == [expected]


def test_validate_records_meta_no_conf(spark_session: SparkSession) -> None:
    """
    Test validation of records when with empty configuration of validation
    of records.
    """
    data = [[1, 'abc'], [2, 'xyz']]
    df = spark_session.createDataFrame(data, ['id', 'c1'])

    #valid, rejected = _validate_records_meta(spark_session, _validation_config(), df)
    valid = _validate_records_meta(spark_session, _validation_config(), df)
    assert df_column(valid, 'id') == [1, 2]
    assert df_column(valid, 'c1') == ['abc', 'xyz']

#    assert rejected.count() == 0
#    assert rejected.columns == ['id', 'c1']


def test_parse_validation_config_pre_transform() -> None:
    """
    Test parsing configuration of validation of records for pre
    transformation validation type.
    """
    f = io.StringIO(VALIDATION_CONFIG)
    config = parse_validation_config(f, 'af_edw_srcparty', 'p_src', ValidationType.PRE_TRANSFORM)

    assert config.pipeline_name == 'af_edw_srcparty'
    assert config.pipeline_task_id == 'p_src'
    assert config.validation_type == ValidationType.PRE_TRANSFORM
    assert config.key == ['src_prty_ident']

    c1, c2, c3, c4 = config.columns

    assert c1.col_name == 'record_deleted_flag'
    assert c1.source_date_format == ''
    assert c1.enum_value == ['0', '1']
    assert c1.validation_name == ValidationName.IS_ENUM

    assert c2.col_name == 'data_src_cd'
    assert c2.source_date_format == ''
    assert c2.enum_value == []
    assert c2.validation_name == ValidationName.IS_MANDATORY

    assert c3.col_name == 'entrp_prty_ident'
    assert c3.source_date_format == ''
    assert c3.enum_value == ['Y', 'N', None]
    assert c3.validation_name == ValidationName.IS_ENUM

    assert c4.col_name == 'src_prty_ident_desc_cd'
    assert c4.source_date_format == ''
    assert c4.enum_value == []
    assert c4.validation_name == ValidationName.IS_MANDATORY


def test_parse_validation_config_post_transform() -> None:
    """
    Test parsing configuration of validation of records for post
    transformation validation type.
    """
    f = io.StringIO(VALIDATION_CONFIG)
    config = parse_validation_config(f, 'af_edw_srcparty', 'p_src', ValidationType.POST_TRANSFORM)

    assert config.pipeline_name == 'af_edw_srcparty'
    assert config.pipeline_task_id == 'p_src'
    assert config.validation_type == ValidationType.POST_TRANSFORM
    assert config.key == ['src_prty_ident']

    c1, c2, c3 = config.columns

    assert c1.col_name == 'data_src_cd'
    assert c1.source_date_format == ''
    assert c1.enum_value == []
    assert c1.validation_name == ValidationName.IS_MANDATORY

    assert c2.col_name == 'data_src_qlfr'
    assert c2.source_date_format == ''
    assert c2.enum_value == []
    assert c2.validation_name == ValidationName.IS_MANDATORY

    assert c3.col_name == 'dcsd_dt'
    assert c3.source_date_format == 'YY/MM/DD'
    assert c3.enum_value == []
    assert c3.validation_name == ValidationName.IS_VALID_DATE


def test_parse_validation_config_empty() -> None:
    """
    Test pasrsing configuration of validation of records with no
    configuration entries.
    """
    f = io.StringIO(VALIDATION_CONFIG)
    config = parse_validation_config(f, 'p_empty', 'p_empty_task_id', ValidationType.POST_TRANSFORM)

    assert config.pipeline_name == 'p_empty'
    assert config.pipeline_task_id == 'p_empty_task_id'
    assert config.validation_type == ValidationType.POST_TRANSFORM

    assert config.key == []
    assert config.columns == []


#
# utility functions
#

def _validation_config(
    *columns: ValidationRecordColumn,
    key_cols: list[str] = ['id'],   
) -> ValidationRecordConfig:
    """
    Create configuration for validation of records
    """
    return ValidationRecordConfig(
        'p_task_id',
        'p_name',
        ValidationType.PRE_TRANSFORM,
        key_cols if columns else [],
        columns,
    )

def _validation_record(
    col: str, validation: ValidationName, enum: list = []
) -> ValidationRecordColumn:
    """
    Create validation record configuration entry.

    :param col: Column name.
    :param validation: Validation name.
    :param enum: Enumeration if validation is enum validation.
    """
    return ValidationRecordColumn(
        col_name = col,
        source_date_format="yyyy-MM-dd",
        enum_value=enum,
        validation_name=validation
    )
