import math

from ..helpers.types.data_type import Datatype
from ..helpers.types.relation_type import RelationType
from .relation_discovery import RelationDiscovery
from typing import Tuple
import pymongo
import re


class ConfigurationBuilder:

    @staticmethod
    def _interpret_relation_type(relation_type: RelationType) -> str:
        if relation_type == RelationType.r_1ton:
            return '1:n'
        if relation_type == RelationType.r_1to1:
            return '1:1'
        if relation_type == RelationType.r_nto1:
            return 'n:1'
        return 'n:m'

    @staticmethod
    def choose_primary_key_candidate(column_infos: dict):
        # Helper function to parse SQL data types
        def parse_sql_type(sql_type):
            if 'varchar' in sql_type or 'character varying' in sql_type:
                length_num = re.search(r'\((\d+)\)', sql_type)
                length_inner = 4095 if not length_num else int(length_num.group(1))
                return 'string', length_inner
            if 'int' in sql_type:
                return 'integer', None
            if 'float' in sql_type or 'double' in sql_type:
                return 'float', None
            if 'numeric' in sql_type:
                precision, scale = (re.search(r'\((\d+),(\d+)\)', sql_type).groups())
                return 'numeric', (int(precision), int(scale))
            if 'boolean' in sql_type:
                return 'boolean', None
            if 'date' in sql_type or 'timestamp' in sql_type:
                return 'datetime', None
            return 'unknown', None

        candidate_scores = []
        for name, sql_type in column_infos.items():
            if name == "id":
                return name
            dtype, length = parse_sql_type(sql_type.lower())
            score = 0
            if dtype == 'integer':
                score += 3  # Prefer integers
            elif dtype == 'string':
                score += 2  # Then strings
                if length:
                    score += 20 / length  # Prefer shorter strings
            elif dtype in ['float', 'numeric']:
                score += 1  # Then floats and numerics

            # No additional scores for other types
            candidate_scores.append((name, score))
        # Sort candidates by score in descending order
        candidate_scores.sort(key=lambda x: x[1], reverse=True)
        if not candidate_scores:
            raise ValueError("No suitable primary key candidates found.")
        return candidate_scores[0][0]  # Return the highest scoring candidate

    @staticmethod
    def build_configuration(collection: pymongo.collection.Collection, schema_name: str = "public", cutoff=1.0) -> \
            Tuple[dict, dict]:
        relation_info = RelationDiscovery.get_relation_info(collection, cutoff)
        mappings = {}
        relations = {}
        for table, info in relation_info.items():
            table_name = f'{schema_name}.{table.replace(".", "_")}'
            mappings[table_name] = {'transfer_options': {'reference_keys': {}}}
            pk_candidates = {}
            for name, column_info in info['columns'].items():
                if not column_info.is_table and not column_info.is_list:
                    sql_definition = column_info.data_type.name
                    if column_info.data_type == Datatype.NOT_ADAPTABLE:
                        mappings[table_name]['transfer_options'].setdefault("conversion_fields", {})[name] = {
                            "source_type": "object", "target_type": "string"}
                        sql_definition = f"CHARACTER VARYING"
                    elif ((column_info.data_type == Datatype.TEXT or column_info.data_type == Datatype.NOT_ADAPTABLE)
                          and column_info.length < 256):
                        length = pow(2, math.ceil(math.log(column_info.length + 1) / math.log(2))) - 1
                        sql_definition = f"CHARACTER VARYING({length})"
                    mappings[table_name]['.'.join(column_info.path)] = f'{name} {sql_definition}'
                    if column_info.unique or len(info["columns"]) == 1:
                        pk_candidates[name] = sql_definition
            mappings[table_name]['transfer_options']['reference_keys'][
                ConfigurationBuilder.choose_primary_key_candidate(pk_candidates)] = "PK"
            relations[table_name] = {}
            for relation in info['relations']:
                relation_type_str = ConfigurationBuilder._interpret_relation_type(relation.relation_type)
                if relation_type_str not in relations[table_name]:
                    relations[table_name][relation_type_str] = {}
                relations[table_name][relation_type_str][f"{schema_name}.{relation.right.replace('.', '_')}"] = {}
            if "alias" in info:
                mappings[table_name]['transfer_options']['alias'] = f"{schema_name}.{info['alias']}"
        return mappings, relations
