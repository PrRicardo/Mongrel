from sqlalchemy.dialects.postgresql import insert


class DatabaseFunctions:
    @staticmethod
    def insert_on_conflict_nothing(table, conn, keys, data_iter):
        # "a" is the primary key in "conflict_table"
        data = [dict(zip(keys, row)) for row in data_iter]
        stmt = insert(table.table).values(data).on_conflict_do_nothing()
        result = conn.execute(stmt)
        return result.rowcount
