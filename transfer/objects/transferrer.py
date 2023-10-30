from transfer.objects.relation_builder import RelationBuilder
from transfer.objects.table_builder import TableBuilder
import json


class Transferrer:
    def prepare_database(self, creation_stmt: str):
        pass


if __name__ == "__main__":
    relation_builder = RelationBuilder()
    with open("../configurations/relations.json") as relation_file:
        relations = relation_builder.calculate_relations(json.load(relation_file))
    table_builder = TableBuilder(relations)
    with open("../configurations/mappings.json") as relation_file:
        table_builder.add_columns_to_relations(json.load(relation_file))
    creation_stmt = table_builder.make_creation_script()
