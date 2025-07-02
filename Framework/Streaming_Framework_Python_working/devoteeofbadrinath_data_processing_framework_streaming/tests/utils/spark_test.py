"""
Unit tests for spark utility functions.
"""

import pytest
from brdj_stream_processing.utils.spark import spark_session, create_spark_properties

def test_create_spark_properties(generic_args: dict, job_args: dict) -> None:
    """
    Test for create spark properties
    """
    spark_properties = create_spark_properties(generic_args, job_args)
    expected_value = {
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
    }
    for key, value in expected_value.items():
        assert spark_properties[key] == value, \
            f"Mismatch for key {key}: {spark_properties[key]} != {value}"
        
#@pytest.mark.skip
def test_spark_session(generic_args, job_args) -> None:
    """
    Test for spark session and spark properties.
    """
    with spark_session("test spark session", generic_args, job_args) as spark:
        actual = spark.sql("select 1 as column_test").collect()[0].column_test
        expected = 1
        assert actual == expected