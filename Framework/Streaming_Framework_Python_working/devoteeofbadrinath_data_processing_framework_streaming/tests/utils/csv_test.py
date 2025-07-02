"""
Unit tests for CSV file utilities.
"""

import pytest
from brdj_stream_processing.utils.csv import csv_column_index

def test_csv_column_index() -> None:
    """
    Test getting column index of a header
    """
    header = ['col1', 'col2', 'col3']

    assert csv_column_index(header, 'col1') == 0
    assert csv_column_index(header, 'col2') == 1
    assert csv_column_index(header, 'col3') == 2

def test_csv_column_index_error() -> None:
    """
    Test if value error is raised when getting column index of a header.
    """
    header = ['col1', 'col2', 'col3']

    with pytest.raises(ValueError) as ctx_ex:
        csv_column_index(header, 'no-col')

    assert str(ctx_ex.value) == 'Column not found: no-col'