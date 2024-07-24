from typing import Tuple

import pymongo

from ..helpers.types.column_info import ColumnInfo
from ..helpers.types.relation_type import RelationType
from ..helpers.types.relation import Relation
from tqdm import tqdm


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
            path_str = '.'.join(path)
            if isinstance(item, dict):
                if path_str not in tables:
                    tables[path_str] = {"columns": {}}
                if key not in tables[base_name]["columns"]:
                    tables[base_name]["columns"][key] = ColumnInfo(expected_values, is_table=True,
                                                                   path=path)
                if key not in processed:
                    processed[key] = ColumnInfo(expected_values, is_table=True, path=path)
                tables[base_name]["columns"][key].is_table = True
                tables[base_name]["columns"][key].add_value(item)
                if item not in processed[key]:
                    processed[key].add_value(item)
                    tables, processed = RelationDiscovery.process_document(item, tables, path_str, expected_values,
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
                    tables[base_name]["columns"][key].add_value(item)
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
            for column_name, column_info in item["columns"].items():
                right_name = column_name if len(column_info.path) <= 1 else '.'.join(column_info.path)
                if column_info.is_table and right_name in tables:
                    if column_info.is_list:
                        if column_info.unique:
                            dict_of_relations[key].append(Relation(key, right_name, RelationType.r_1ton))
                        else:
                            dict_of_relations[key].append(Relation(key, right_name, RelationType.r_ntom))
                    else:
                        if column_info.unique:
                            dict_of_relations[key].append(Relation(key, right_name, RelationType.r_1to1))
                        else:
                            dict_of_relations[key].append(Relation(key, right_name, RelationType.r_nto1))
                elif column_info.is_list and right_name in tables:
                    dict_of_relations[key].append(Relation(key, right_name, RelationType.r_ntom))
        return dict_of_relations

    @staticmethod
    def handle_doubles(relations: dict) -> dict:
        for key, item in relations.items():
            if 'alias' not in item:
                doubles = [double for double in RelationDiscovery.check_doubles(item, relations) if key != double]
                if len(doubles) > 0:
                    alias = key.split('.')[-1]
                    if alias in relations:
                        alias = alias + '_alias'
                    item['alias'] = alias
                    for double in doubles:
                        relations[double]['alias'] = alias
                    doubles.append(key)
                    print("Duplicates of " + alias + ": " + str(doubles))
        return relations

    @staticmethod
    def _remove_empty_tables(tables):
        remove = []
        for name, table in tables.items():
            existing = False
            for _, info in table["columns"].items():
                if not info.is_table:
                    existing = True
                    break
            if not existing:
                remove.append(name)
        for name in remove:
            del tables[name]
        return tables

    @staticmethod
    def _add_lists(tables):
        to_add = []
        for key, item in tables.items():
            for col_name, col_info in item["columns"].items():
                if col_info.is_list and not col_info.is_table:
                    path_str = '.'.join(col_info.path)
                    if path_str not in tables:
                        to_add.append({path_str: {"columns": {col_name: ColumnInfo(expected_values=0,
                                                                                   path=col_info.path,
                                                                                   datatype=col_info.data_type,
                                                                                   length=col_info.length)}}})
        for item in to_add:
            for key, value in item.items():
                tables[key] = value
        return tables

    @staticmethod
    def get_relation_info(collection: pymongo.collection.Collection, cutoff=1.0) -> dict:
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
        tables = RelationDiscovery._add_lists(tables)
        tables = RelationDiscovery._remove_empty_tables(tables)
        relations = rellie.interpret_value_appearances(tables)
        for key, relations in relations.items():
            tables[key]["relations"] = relations
        tables = RelationDiscovery.handle_doubles(tables)
        return tables
