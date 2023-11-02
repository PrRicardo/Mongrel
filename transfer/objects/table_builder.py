import json

from transfer.helpers.exceptions import MalformedMappingException
from transfer.objects.relation import Relation
from transfer.objects.relation_builder import RelationBuilder


class TableBuilder:
    relations: list[Relation]

    def __init__(self, relations_vals: list[Relation]):
        self.relations = relations_vals

    def prepare_rel_dict(self):
        rettich = {}
        for rel in self.relations:
            rettich[rel.info] = rel
        return rettich

    def add_columns_to_relations(self, mapping_dict: dict):
        for relation in self.relations:
            if str(relation.info) in mapping_dict:
                relation.parse_column_dict(mapping_dict[str(relation.info)])
            else:
                raise MalformedMappingException(
                    "The mapping file does not contain mapping information on " + str(relation.info))

    def create_nm_script(self) -> str:
        creation_stmt = ""
        for relation in relations:
            if 'n:m' in relation.relations:
                for nm_relation in relation.relations['n:m']:
                    creation_stmt += relation.create_nm_table(relations[relations.index(nm_relation)])
        return creation_stmt

    def create_schema_script(self) -> str:
        creation_stmt = ""
        unique_schemas = []
        for relation in self.relations:
            if relation.info.schema not in unique_schemas:
                unique_schemas.append(relation.info.schema)
        for schema in unique_schemas:
            creation_stmt = creation_stmt + f'CREATE SCHEMA IF NOT EXISTS "{schema}";\n\n'
        return creation_stmt

    def make_creation_script(self) -> str:
        creation_stmt = self.create_schema_script()
        done = []
        for relation in relations:
            if "n:1" not in relation.relations:
                creation_stmt = creation_stmt + relation.make_creation_script(self.prepare_rel_dict())
                done.append(relation)
        while len(done) != len(relations):
            progress = 0
            for relation in relations:
                if "n:1" in relation.relations:
                    brrr = False
                    for kek in relation.relations["n:1"]:
                        if kek not in done:
                            brrr = True
                    if not brrr:
                        creation_stmt = creation_stmt + relation.make_creation_script(self.prepare_rel_dict())
                        done.append(relation)
                        progress += 1
            if progress == 0:
                raise MalformedMappingException("There are cycles in the n:1 relations and therefore "
                                                "no foreign key could be generated.")
        creation_stmt += self.create_nm_script()
        return creation_stmt


if __name__ == "__main__":
    relation_builder = RelationBuilder()
    with open("configurations/relations.json") as relation_file:
        relations = relation_builder.calculate_relations(json.load(relation_file))
    table_builder = TableBuilder(relations)
    with open("configurations/mappings.json") as relation_file:
        table_builder.add_columns_to_relations(json.load(relation_file))
    creation_stmt = table_builder.make_creation_script()
    pass
