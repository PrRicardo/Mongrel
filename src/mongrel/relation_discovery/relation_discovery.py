from typing import Tuple

import pymongo

from .helpers.bloom_filter import BloomFilter
from ..helpers.relation_type import RelationType
from ..relation_discovery.relation import Relation
from tqdm import tqdm


class ColumnInfo:
    values: BloomFilter
    unique: bool
    is_table: bool
    is_list: bool
    path: list

    def __init__(self, expected_values: int, is_table: bool = False, is_list: bool = False,
                 false_positive_acceptance: int = 0.000000001, path: list = None):
        self.path = []
        self.path.append(path)
        self.values = BloomFilter(expected_values, false_positive_acceptance)
        self.unique = True
        self.is_table = is_table
        self.is_list = is_list

    def add_value(self, value, path) -> bool:
        if path not in self.path:
            self.path.append(path)
        if value in self.values:
            self.unique = False
            return True
        self.values.add(value, checked=True)
        return False

    def __contains__(self, item):
        return item in self.values


class RelationDiscovery:

    @staticmethod
    def process_document(document: dict, tables: dict, base_name: str, expected_values: int,
                         processed: dict = None, current_path: list = None) -> Tuple[dict, dict]:
        """
        This function calculates for all values if they are unique and whether the values of underlying documents
        are unique. The final structure looks as follows:
        Collection_name
            - columns
                - collection_field infos
                - subdocument as column_info
        subdocument
            - columns
                - subdocument field
                - subsubdocuments as column_info
        subsubdocumnent
        ...
        :param document: the document to process
        :param tables: the tables dictionary up till now
        :param base_name: the name of the base_document which gets crawled
        :param expected_values: amount of expected values to create the bloom filters
        :param processed: dictionary that contains all processed documents
        :return:
        """
        if processed is None:
            processed = {}
        if current_path is None:
            current_path = []
        for key, item in document.items():
            path = current_path + [key]
            if isinstance(item, dict):
                if key not in tables:
                    tables[key] = {"columns": {}}
                if key not in tables[base_name]["columns"]:
                    tables[base_name]["columns"][key] = ColumnInfo(expected_values, is_table=True,
                                                                   path=path)
                if key not in processed:
                    processed[key] = ColumnInfo(expected_values, is_table=True, path=path)
                tables[base_name]["columns"][key].add_value(item, path)
                if item not in processed[key]:
                    processed[key].add_value(item, path)
                    tables, processed = RelationDiscovery.process_document(item, tables, key, expected_values,
                                                                           processed, current_path=path)
            elif isinstance(item, list):
                if key not in tables[base_name]["columns"]:
                    factor = 1
                    if len(item) > 0:
                        factor = len(item)
                    tables[base_name]["columns"][key] = ColumnInfo(factor * expected_values, is_list=True,
                                                                   path=path)
                for list_item in item:
                    tables, processed = RelationDiscovery.process_document({key: list_item}, tables, base_name,
                                                                           expected_values, processed,
                                                                           current_path=current_path)
            else:
                if item is not None:
                    if key not in tables[base_name]["columns"]:
                        tables[base_name]["columns"][key] = ColumnInfo(expected_values, path=path)
                    tables[base_name]["columns"][key].add_value(item, path)
        return tables, processed

    @staticmethod
    def has_same_columns(columns: list[str], to_comp: dict) -> bool:
        if len(columns) != len(to_comp):
            return False
        for key, _ in to_comp.items():
            if key not in columns:
                return False
        return True

    @staticmethod
    def check_doubles(to_check: dict, tables: dict) -> list[str]:
        col_list = [key for key, _ in to_check["columns"].items()]
        for key, item in tables.items():
            if RelationDiscovery.has_same_columns(col_list, item["columns"]):
                yield key

    @staticmethod
    def interpret_value_appearances(tables: dict) -> dict[str, list[Relation]]:
        dict_of_relations = {}
        for key, item in tables.items():
            if key not in dict_of_relations:
                dict_of_relations[key] = []
            for double in RelationDiscovery.check_doubles(item, tables):
                print(f"{key} might be a double of {double}")
            for column_name, column_info in item["columns"].items():
                if column_info.is_table:
                    if column_info.is_list:
                        if column_info.unique:
                            dict_of_relations[key].append(Relation(key, column_name, RelationType.r_1ton))
                        else:
                            dict_of_relations[key].append(Relation(key, column_name, RelationType.r_ntom))
                    else:
                        if column_info.unique:
                            dict_of_relations[key].append(Relation(key, column_name, RelationType.r_1to1))
                        else:
                            dict_of_relations[key].append(Relation(key, column_name, RelationType.r_nto1))
        return dict_of_relations

    @staticmethod
    def calculate_relations(collection: pymongo.collection.Collection, cutoff=1.0) -> dict[
        str, list[Relation]]:
        if cutoff < 0 or cutoff > 1:
            cutoff = 1.0
        tables = {collection.name: {"columns": {}}}
        rellie = RelationDiscovery()
        processed = {}
        expected = collection.count_documents({})
        counter = 0
        for doc in tqdm(collection.find()):
            counter += 1
            tables, processed = rellie.process_document(doc, tables, collection.name, expected,
                                                        processed)
            if cutoff < 1 and counter > cutoff * expected:
                break
        relations = rellie.interpret_value_appearances(tables)
        return relations
