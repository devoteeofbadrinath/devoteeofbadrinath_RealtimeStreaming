import logging

logger = logging.getLogger(__name__)

def csv_column_index(header: list, column:str) -> int:

    logger.debug('header: {}'.format(header))
    try:
        return header.index(column)
    except ValueError as ex:
        raise ValueError(f'Column not found: {column}') from ex