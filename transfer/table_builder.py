import json

from transfer.exceptions import MalformedMappingException
from transfer.relation import Relation
from transfer.relation_builder import RelationBuilder


class TableBuilder:
    relations: list[Relation]

    def __init__(self, relations_vals: list[Relation]):
        self.relations = relations_vals

    def add_columns_to_relations(self, mapping_dict: dict):
        for relation in self.relations:
            if str(relation.info) in mapping_dict:
                relation.parse_column_dict(mapping_dict[str(relation.info)])
            else:
                raise MalformedMappingException(
                    "The mapping file does not contain mapping information on " + str(relation.info))

if __name__ == "__main__":
    relation_builder = RelationBuilder()
    with open("configurations/relations.json") as relation_file:
        relations = relation_builder.calculate_relations(json.load(relation_file))
    table_builder = TableBuilder(relations)
    with open("configurations/mappings.json") as relation_file:
        table_builder.add_columns_to_relations(json.load(relation_file))
    pass