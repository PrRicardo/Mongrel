from enum import Enum


class ConflictHandling(Enum):
    """
    This enum describes the different choices of error handling.
    """
    NONE = 1
    """
    Choose value None to add ON CONFLICT DO NOTHING at the end of its inserts.
    """
    TRUNCATE = 2
    """
    TRUNCATE truncates previous tables with the same schema and table name as the target tables
    """
    DROP = 3
    """
    DROP previous tables with the same schema and table name as the target tables
    """
