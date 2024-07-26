class EmptyTableError(Exception):
    """Raised when the data table is empty"""
    pass

class InsufficientRowsError(Exception):
    """Raised when the table has less than min number of rows"""
    pass

class NoPatternsException(Exception):
    """Raised when no patterns were found"""
    pass