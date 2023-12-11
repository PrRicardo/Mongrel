"""
Helper functions for the pandas upload functionality.
"""
from sqlalchemy.dialects.postgresql import insert


class DatabaseFunctions:
    @staticmethod
    def insert_on_conflict_nothing(table, conn, keys, data_iter):
        """
        This method is used to use the on conflict do nothing functionality on the database.
        It's being called by pandas anyways, so let's just not comment this any further.
        """
        data = [dict(zip(keys, row)) for row in data_iter]
        stmt = insert(table.table).values(data).on_conflict_do_nothing()
        result = conn.execute(stmt)
        return result.rowcount
