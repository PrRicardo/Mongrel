from typing import Any

import pandas as pd

from ..helpers.constants import AUTO_ID,ROOT_COLUMN
from ..helpers.hashing import hash_list, hash_dict, hash_scalar
from ..helpers.relation_types import RelationType
from src.mongrel_transferrer.structures.relation import Relation


class ElementStructure:
    parent: Any # ElementStructure
    children: dict[int, Any] # ElementStructure
    hashes: set
    parent_hashes: set
    path: list[str]
    parent_relation: Relation
    rows: list[dict]
    parent_relation_dict: dict[int, set]

    def __init__(self, parent: Any = None, path: list[str] = None):
        self.parent = parent
        self.children = {}
        self.hashes = set()
        self.parent_hashes = set()
        self.path = path or [ROOT_COLUMN]
        self.relations = []
        self.rows = []
        self.parent_relation_dict = {}
        if parent is not None:
            self.parent_relation = Relation(path[-2], path[-1])

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

    def _child_handling(self, sub_element: Any, path: list[str], own_hash: int):
        path_hash = hash_list(path)
        if path_hash not in self.children:
            self.children[path_hash] = ElementStructure(self, path)
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

    def parent_relations_to_df(self):
        to_add = []
        for parent_id, child_ids in self.parent_relation_dict:
            for child_id in child_ids:
                to_add.append({self.path[-2]:parent_id, self.path[-1]:child_id})
        return pd.DataFrame(to_add)


    def add_doc(self, sub_element: Any, parent_hash: int = None):
        if isinstance(sub_element, dict):
            self._dict_handling(sub_element, parent_hash)
        else:
            self._scalar_handling(sub_element, parent_hash)
