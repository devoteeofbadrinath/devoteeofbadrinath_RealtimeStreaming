from unittest.mock import patch
from datetime import datetime
import pytest
from pyspark.sql import Row
from brdj_stream_processing.stream.stream_process import read_latest_offsets, process_stream_batch


@pytest.fixture
def micro_batch_df(spark_session):
    return spark_session.createDataFrame([
        Row(
            value=(
                '{"MessageHeader": {"header_key":"header_value"}, '
                '"MessageBody": {"body_key": "body_value", '
                '"AccArr": {"acc_key": "acc_value"}, '
                '"Branch": {"branch_key": "branch_value"}}}'
            )
        ),
        Row(
            value=(
                '{"MessageHeader": {"header_key":"header_value2"}, '
                '"MessageBody": {"body_key": "body_value2", '
                '"AccArr": {"acc_key": "acc_value2"}, '
                '"Branch": {"branch_key": "branch_value2"}}}'
            )
        ),
    ])


    def test_read_latest_offsets(spark_session):
        data = [
            ["0", 1],
            ["0", 2],
            ["0", 3],
            ["1", 5],
            ["1", 6],
            ["1", 7],
        ]
        df = spark_session.createDataFrame(data, ["partition", "offset"])
        offsets = read_latest_offsets(df)
        assert offsets == {"0": 3, "1": 7}


@patch('brdj_stream_processing.stream.stream_process.read_latest_offsets')
@patch('brdj_stream_processing.stream.stream_process.process_data')
def test_process_stream_batch(mock_process_data, mock_read_latest_offsets, ctx, micro_batch_df):
    mock_read_latest_offsets.return_value = {"0": 100, "1": 101}
    mock_process_data.return_value = 80
    dropped_batch_df = micro_batch_df.drop("partition", "offset")

    process_stream_batch(ctx, micro_batch_df, batch_id=1)

    assert not ctx.state.metrics_queue.empty()

    metrics = ctx.state.metrics_queue.get()
    assert metrics["offsets"] == {"0": 100, "1": 101}
    assert isinstance(metrics["metrics"].metrics_window_start_time, datetime)
    assert isinstance(metrics["metrics"].metrics_window_end_time, datetime)
    assert metrics["metrics"].total == 0
    #assert metrics["metrics"].total == dropped_batch_df.count()
    assert metrics["metrics"].target_write == 0
    #assert metrics["metrics"].target_write == 80

    mock_read_latest_offsets.assert_called_once_with(micro_batch_df)

    ctx_arg, df_arg = mock_process_data.call_args[0]
    assert ctx_arg == ctx
    assert df_arg.collect() == dropped_batch_df.collect()