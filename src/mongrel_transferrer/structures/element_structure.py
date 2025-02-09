from typing import Any

import pandas as pd
import sqlalchemy
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

from ..helpers.constants import AUTO_ID, ROOT_COLUMN
from ..helpers.hashing import hash_list, hash_dict, hash_scalar
from ..helpers.relation_types import RelationType
from src.mongrel_transferrer.structures.relation import Relation
from ..helpers.sql import get_columns, is_valid_column, get_col_type


def _add_missing_columns(data: pd.DataFrame, engine: sqlalchemy.Engine, table: str, schema):
    columns = get_columns(engine, table, schema)
    required_columns = [col for col in data.columns if col not in columns]
    if any(not is_valid_column(col) for col in required_columns):
        raise AssertionError("Is this... SQL Injection?")
    alter_query = f"ALTER TABLE {schema}.{table} "
    alter_query += ", ".join(f"ADD COLUMN {col} {get_col_type(data[col].dtype)}" for col in required_columns)
    with engine.connect() as conn:
        conn.execute(text(alter_query))
        conn.commit()
    return

def _upload(data: list[dict], df_pks: list[str], table: str, schema: str,
            engine: sqlalchemy.engine, replace: bool):
    pks = {}
    for pk in df_pks:
        pks[pk] = postgresql.BIGINT
    data = pd.DataFrame(data)
    try:
        data.to_sql(name=table, schema=schema, if_exists="append" if not replace else "replace",
                    con=engine, dtype=pks)
    except sqlalchemy.exc.ProgrammingError as e:
        if not 'psycopg2.errors.UndefinedColumn' in str(e):
            raise e
        _add_missing_columns(data, engine, table, schema)
        data.to_sql(name=table, schema=schema, if_exists="append" if not replace else "replace",
                    con=engine, dtype=pks)

class ElementStructure:
    parent: Any  # ElementStructure
    children: dict[int, Any]  # ElementStructure
    hashes: set
    parent_hashes: set
    path: list[str]
    parent_relation: Relation
    rows: list[dict]
    parent_relation_dict: dict[int, set]
    identifier: str
    identifier_lookup_ref: set

    def __init__(self, identifier: str, identifier_lookup_ref: set, parent: Any = None, path: list[str] = None):
        self.parent = parent
        self.children = {}
        self.identifier = identifier
        self.hashes = set()
        self.parent_hashes = set()
        self.path = path or [ROOT_COLUMN]
        self.relations = []
        self.rows = []
        self.parent_relation_dict = {}
        if parent is not None:
            self.parent_relation = Relation(self.parent.identifier, self.identifier)
        self.identifier_lookup_ref = identifier_lookup_ref

    def _preamble(self, hash_id: int, parent_hash: int) -> bool:
        res = hash_id in self.hashes
        if self.parent is None:
            return res
        self.parent_relation_dict.setdefault(parent_hash, set()).add(hash_id)
        if self.parent_relation.type == RelationType.many_to_many:
            return res
        if parent_hash not in self.parent_hashes:
            if res:
                self.parent_relation.type = RelationType.many_to_many
        elif not res:
            self.parent_relation.type = RelationType.one_to_many
        self.parent_hashes.add(parent_hash)
        self.hashes.add(hash_id)
        return res

    def _make_child_identifier(self, path) -> str:
        counter = -1
        while True:
            candidate = "_".join(path[counter:])
            if candidate not in self.identifier_lookup_ref:
                self.identifier_lookup_ref.add(candidate)
                return candidate
            counter -= 1

    def _child_handling(self, sub_element: Any, path: list[str], own_hash: int):
        path_hash = hash_list(path)
        if path_hash not in self.children:
            self.children[path_hash] = ElementStructure(self._make_child_identifier(path),
                                                        self.identifier_lookup_ref, self, path)
        if isinstance(sub_element, list):
            for element in sub_element:
                self.children[path_hash].add_doc(element, own_hash)
        if isinstance(sub_element, dict):
            self.children[path_hash].add_doc(sub_element, own_hash)

    def _scalar_handling(self, sub_element: Any, parent_hash: int):
        hash_id = hash_scalar(sub_element)
        if self._preamble(hash_id, parent_hash):
            return
        key = self.path[-1] if self.parent is not None else ROOT_COLUMN
        self.rows.append({key: sub_element})

    def _dict_handling(self, sub_element: dict, parent_hash: int):
        to_process = sorted(sub_element.items())
        hash_id = hash_dict(sub_element)
        if self._preamble(hash_id, parent_hash):
            return
        to_add = {AUTO_ID: hash_id}
        for key, item in to_process:
            if isinstance(item, list) or isinstance(item, dict):
                self._child_handling(sub_element[key], self.path + [key], hash_id)
                continue
            to_add[key] = item
        self.rows.append(to_add)

    def to_df(self):
        return pd.DataFrame(self.rows)

    def parent_relations_to_list(self):
        to_add = []
        for parent_id, child_ids in self.parent_relation_dict.items():
            for child_id in child_ids:
                to_add.append({self.parent.identifier: parent_id, self.identifier: child_id})
        return to_add

    def add_doc(self, sub_element: Any, parent_hash: int = None):
        if isinstance(sub_element, dict):
            self._dict_handling(sub_element, parent_hash)
        else:
            self._scalar_handling(sub_element, parent_hash)

    def __len__(self):
        return len(self.rows)

    def largest_buffer_length(self):
        curr_max = max(len(self.rows), sum(len(rel) for rel in self.parent_relation_dict.items()))
        if len(self.children) > 0:
            return max(curr_max, max(child.largest_buffer_length() for child in self.children.values()))
        return curr_max

    def write(self, schema: str, engine: sqlalchemy.engine, replace: bool):
        _upload(self.rows, [AUTO_ID], self.identifier, schema, engine, replace)
        for child in self.children.values():
            child.write(schema, engine, replace)
        if self.parent is not None:
            _upload(self.parent_relations_to_list(), [self.parent.identifier, self.identifier],
                    f"{self.parent.identifier}2{self.identifier}", schema, engine, replace)
        self.rows = []
        self.parent_relation_dict = {}