"""
Manages the creation of tables with a creation statement
"""

import json
from mongrel.helpers.exceptions import MalformedMappingException
from mongrel.objects.relation import Relation
from mongrel.objects.relation_builder import RelationBuilder


class TableBuilder:
    _relations: list[Relation]
    _rel_dict: dict

    def __init__(self, relations_vals: list[Relation], relation_file_path: str):
        """
        Initializes the class which is used to make the creation statement
        :param relations_vals: a list of all relations which
        :param relation_file_path: filepath to the relation
        """
        self._relations = relations_vals
        self._rel_dict = self.prepare_rel_dict()
        with open(relation_file_path) as relation_file:
            self.add_columns_to_relations(json.load(relation_file))
        self._relations = self._prepare_nm_relations(relations_vals)

    def get_relations(self):
        """
        Returns the relation from the table_builder
        If the relations are not yet prepared they are prepared
        :return: the list of prepared relations
        """
        for relation in self._relations:
            relation.prepare_columns(self._rel_dict)
        return self._relations

    def _prepare_nm_relations(self, relations_vals):
        """
        Creates the nm tables that are required to emulate n:m relations
        :param relations_vals: all tables with their relations
        :return: the new relations as a list
        """
        for relation in relations_vals:
            if 'n:m' in relation.relations:
                for nm_relation in relation.relations['n:m']:
                    nm_table = relation.create_nm_table(
                        relations_vals[relations_vals.index(nm_relation)], self._rel_dict)
                    if nm_table not in relations_vals:
                        relations_vals.append(nm_table)
        return relations_vals

    def prepare_rel_dict(self):
        """
        Creates a dict to enable relation lookup by Relation Info
        :return: the dict
        """
        rettich = {}
        for rel in self._relations:
            rettich[rel.info] = rel
        return rettich

    def add_columns_to_relations(self, mapping_dict: dict):
        """
        Parses the mapping json configuration file for all relations
        :param mapping_dict: the json configuration file as a dict
        :raise MalformedMappingException: If there is no information on one of the relations
        """
        for relation in self._relations:
            if str(relation.info) in mapping_dict:
                relation.parse_column_dict(mapping_dict[str(relation.info)])
            else:
                raise MalformedMappingException(
                    "The mapping file does not contain mapping information on " + str(relation.info))

    def create_schema_script(self) -> str:
        """
        Creates all unique schemas with the creation stmt
        :return: the creation statement for the schemas
        """
        creation_stmt_inner = ""
        unique_schemas = set()
        for relation in self._relations:
            unique_schemas.add(relation.info.schema if not relation.alias else relation.alias.schema)
        for schema in unique_schemas:
            creation_stmt_inner = creation_stmt_inner + f'CREATE SCHEMA IF NOT EXISTS "{schema}";\n\n'
        return creation_stmt_inner

    def make_creation_script(self) -> str:
        """
        Builds the entire relationscript which can be used to initialize the database.
        :return: The creation statement
        :raise MalformedMappingException: If there are cyclic dependencies
        """
        creation_stmt_inner = self.create_schema_script()
        done = []
        # Creates the tables without n:1 relations
        for relation in self._relations:
            if relation not in done:
                if not relation.alias:
                    if "n:1" not in relation.relations:
                        creation_stmt_inner = creation_stmt_inner + relation.make_creation_script(self._rel_dict)
                        done.append(relation)
                else:
                    if "n:1" not in relation.relations:
                        alias_relations = relation.get_alias_relations(self._relations)
                        fine = True
                        for alias_relation in alias_relations:
                            if "n:1" in alias_relation.relations:
                                fine = False
                                break
                        if fine:
                            creation_stmt_inner = creation_stmt_inner + relation.make_creation_script(self._rel_dict,
                                                                                                      alias_relations)
                            done.append(relation)
                            done.extend(alias_relations)
        # Creates the tables with n:1 relations since they depend on each other
        while len(done) != len(self._relations):
            progress = 0
            for relation in self._relations:
                if "n:1" in relation.relations and relation not in done:
                    has_uninitialized_references = False
                    for reference in relation.relations["n:1"]:
                        if reference not in done:
                            has_uninitialized_references = True
                    if not has_uninitialized_references:
                        if not relation.alias:
                            creation_stmt_inner = creation_stmt_inner + relation.make_creation_script(self._rel_dict)
                            done.append(relation)
                            progress += 1
                        else:
                            alias_relations = relation.get_alias_relations(self._relations)
                            for rellie in alias_relations:
                                for reference in rellie.relations["n:1"]:
                                    if reference not in done:
                                        has_uninitialized_references = True
                            if not has_uninitialized_references:
                                creation_stmt_inner = creation_stmt_inner + relation.make_creation_script(
                                    self._rel_dict)
                                done.append(relation)
                                progress += 1
            if progress == 0:
                raise MalformedMappingException("There are cycles in the n:1 relations and therefore "
                                                "no foreign key could be generated.")
        return creation_stmt_inner


if __name__ == "__main__":
    relation_builder = RelationBuilder()
    with open("configurations/relations.json") as relation_file:
        with open("configurations/mappings.json") as mapping_file:
            relations = relation_builder.calculate_relations(json.load(relation_file), json.load(mapping_file))
    table_builder = TableBuilder(relations, "configurations/mappings.json")
    creation_stmt = table_builder.make_creation_script()
    pass
