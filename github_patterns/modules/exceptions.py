class EmptyTableError(Exception):
    """Raised when the table is empty"""
    pass

class InsufficientRowsError(Exception):
    """Raised when the table has less than 200 rows"""
    pass