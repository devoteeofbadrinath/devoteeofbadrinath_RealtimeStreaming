"""
BRDJ project exceptions.
"""


class BRDJException(Exception):
    """Base exception for BRDJ Stream data Processing project."""


class OffsetNotFoundError(BRDJException):
    """
    Exception raised if offset is not found.
    """  
    

class RecordValidationError(BRDJException):
    """
    Exception raised on validation of records failure.
    """