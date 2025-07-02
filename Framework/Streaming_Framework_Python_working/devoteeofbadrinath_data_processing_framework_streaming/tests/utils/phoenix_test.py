"""
Unit tests for Phoenix database utilities.
"""

import pytest
from unittest import mock
from brdj_stream_processing.utils.phoenix import read_from_phoenix, \
    read_from_phoenix_with_jdbc

#@pytest.mark.skip
def test_read_from_phoenix() -> None:
    """
    Test reading data from Phoenix table.
    """
    spark = mock.MagicMock()
    zk_url = 'zk://zk-url:33'
    _ = read_from_phoenix(spark, 'schema.a_table', zk_url)

    mock_read = spark.read.format()
    mock_read.option.assert_called_once_with('table', 'schema.a_table')
    mock_read.option().option.assert_called_once_with('zkUrl', 'zk://zk-url:33')
    mock_read.option().option().load.assert_called_once()

def test_read_from_phoenix_with_jdbc() -> None:
    """
    Test reading data from Phoenix table.
    """
    spark = mock.MagicMock()
    zk_url = 'zk://zk-url:33'
    query = 'SELECT * FROM schema.table'
    driver = 'org.apache.phoenix.jdbc.PhoenixDriver'
    _ = read_from_phoenix_with_jdbc(spark, query, zk_url, driver)

    mock_read = spark.read.format()
    mock_read.option.assert_called_once_with('dbtable', 'SELECT * FROM schema.table')
    mock_read.option().option.assert_called_once_with('url', f'jdbc:phoenix:{zk_url}:/hbase')
    mock_read.option().option().option.assert_called_once_with('driver', driver)
    mock_read.option().option().option().load.assert_called_once()