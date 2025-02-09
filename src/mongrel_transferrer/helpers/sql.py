import pandas as pd
import sqlalchemy
import re

def get_columns(engine: sqlalchemy.Engine, table: str, schema: str):
    query = ("SELECT column_name FROM information_schema.columns WHERE table_name = %(table)s "
             "AND table_schema = %(schema)s;")
    return pd.read_sql(query, engine, params={"table": table, "schema": schema})['column_name'].tolist()

def is_valid_column(column_name:str):
    return bool(re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', column_name))

def get_col_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_string_dtype(dtype):
        return "VARCHAR"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    else:
        raise AssertionError(f"Type {str(dtype)} could not be parsed")