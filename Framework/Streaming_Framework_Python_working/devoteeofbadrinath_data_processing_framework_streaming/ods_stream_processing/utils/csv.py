"""
Utility functions for data read from CSV files.
"""

import logging


logger = logging.getLogger(__name__)


# TODO: Python 3.6 tech debt
# def csv_column_index(header: list[str], column: str) -> int:
def csv_column_index(header: list, column:str) -> int:
    """
    Get index of CSV column in the header by the the column name.

    :param header: List of column names in a CSV file header.
    :param column: Column name.
    """
    logger.debug('header: {}'.format(header))
    try:
        return header.index(column)
    except ValueError as ex:
        raise ValueError(f'Column not found: {column}') from ex