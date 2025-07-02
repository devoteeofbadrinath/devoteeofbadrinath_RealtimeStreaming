import json
from pyspark.sql import Row
from pyspark.sql.streaming import DataStreamReader
from brdj_stream_processing.stream.input_module import read_stream_from_kafka, read_from_kafka, \
    read_data_from_source
from brdj_stream_processing.stream.stream_types import ApplicationContext

from unittest import mock


@mock.patch('brdj_stream_processing.stream.input_module.parse_avro_data')
def test_read_stream_from_kafka(mock_parse_avro_data, stream_config):
    ctx = mock.MagicMock()
    ctx.config = stream_config

    mock_read_stream = mock.MagicMock(DataStreamReader)
    mock_read_stream.format.return_value = mock_read_stream
    mock_read_stream.option.return_value = mock_read_stream

    mock_spark = ctx.spark
    mock_spark.readStream = mock_read_stream

    mock_valid_df = mock.MagicMock()
    mock_invalid_df = mock.MagicMock()
    mock_parse_avro_data.return_value = (mock_valid_df, mock_invalid_df)

    mock_micro_batch_fn = mock.MagicMock(name='micro_batch_fn')

    read_stream_from_kafka(
        ctx,
        mock_micro_batch_fn,
        "earliest",
        mock.MagicMock(),
        options={
            'maxOffsetsPerTrigger': '1000',
            'failOnDataLoss': 'false',
        }
    )

    jaas = "org.apache.kafka.common.security.plain.PlainLoginModule required"
    expected_calls = [
        ('format',('kafka',),{}),
        ('option', ('subscribe', stream_config.kafka_topic), {}),
        ('option', ('kafka.bootstrap.servers', stream_config.kafka_bootstrap_servers), {}),
        ('option', ('startingOffsets', 'earliest'), {}),
        ('option', ('kafka.security.protocol', 'SASL_SSL'), {}),
        ('option', ('kafka_sasl_mechanism', 'PLAIN'), {}),
        ('option', ('kafka.sasl.jass.config',
                    f'{jaas} username="dummy_id" password="dummy_password";'), {}),
        ('option', ('value.deserializer', "io.confluent.kafka.serializers.KafkaAvroDeserializer"), {}),
        ('option', ('maxOffsetsPerTrigger', "1000"), {}),
        ('option', ('failOnDataLoss', "false"), {}),
        ('load', (), {})
    ]

    assert expected_calls == mock_spark.readStream.method_calls
    mock_valid_df.writeStream.foreachBatch.assert_called_once_with(mock_micro_batch_fn)
    mock_valid_df.writeStream.foreachBatch.return_value.start.assert_called_once()

    mock_invalid_df.writeStream.foreachBatch.assert_called_once()
    mock_invalid_df.writeStream.foreachBatch.return_value.start.assert_called_once()


@mock.patch('brdj_stream_processing.stream.input_module.read_stream_from_kafka')
def test_read_stream_kafka(mock_read_stream_from_kafka, ctx: ApplicationContext, record_schema):
    #Mock starting offsets and micro batch function
    starting_offsets = {"0":10, "1": 20}
    micro_batch_fn = mock.MagicMock()

    # Call the function and check the result
    read_from_kafka(ctx, starting_offsets, record_schema, micro_batch_fn)
    mock_read_stream_from_kafka.assert_called_once_with(
        ctx,
        micro_batch_fn,
        json.dumps({"test_topic": starting_offsets}),
        record_schema
    )


@mock.patch('brdj_stream_processing.stream.input_module.read_from_phoenix_with_jdbc')
def test_read_data_from_source(mock_read_from_phoenix_with_jdbc, spark_session):
    phoenix_data_source = {
        "job_schema": "test_schema",
        "table": "test_table",
        "zkurl": "localhost:2131",
        "src_id_col": "id",
        "tgt_id_col": "id",
        "phoenix_driver": "org.apache.phoenix.jdbc.PhoenixDriver"
    }
    columns = ["id", "value"]

    # Create a test incoming dataframe
    incoming_data = [
        Row(id=1, value='value1'),
        Row(id=2, value='value2')
    ]
    incoming_df = spark_session.createDataFrame(incoming_data)
    phoenix_data = [
        Row(id=1, value='name1'),
        Row(id=2, value='name2')
    ]
    mock_df = spark_session.createDataFrame(phoenix_data)
    mock_read_from_phoenix_with_jdbc.return_value = mock_df

    # Call the function and check the result
    result_df = read_data_from_source(spark_session, phoenix_data_source, columns, incoming_df)
    expected_data = [
        Row(id=1, name='name1'),
        Row(id=2, name='name2')
    ]
    expected_df = spark_session.createDataFrame(expected_data)

    assert result_df.collect() == expected_df.collect()
    mock_read_from_phoenix_with_jdbc.assert_called_once_with(
        spark_session,
        f"{phoenix_data_source['job_schema']}.{phoenix_data_source['table']}",
        phoenix_data_source["zkurl"],
        phoenix_data_source["phoenix_driver"]
    )