import json

from transfer.helpers.exceptions import MalformedMappingException
from transfer.objects.relation import Relation
from transfer.objects.relation_builder import RelationBuilder


class TableBuilder:
    _relations: list[Relation]
    _rel_dict: dict

    def __init__(self, relations_vals: list[Relation], relation_file_path: str):
        self._relations = relations_vals
        self._rel_dict = self.prepare_rel_dict()
        with open(relation_file_path) as relation_file:
            self.add_columns_to_relations(json.load(relation_file))
        self._relations = self._prepare_nm_relations(relations_vals)

    def get_relations(self):
        for relation in self._relations:
            relation.prepare_columns(self._rel_dict)
        return self._relations

    def _prepare_nm_relations(self, relations_vals):
        for relation in relations_vals:
            if 'n:m' in relation.relations:
                for nm_relation in relation.relations['n:m']:
                    nm_table = relation.create_nm_table(
                        relations_vals[relations_vals.index(nm_relation)], self._rel_dict)
                    if nm_table not in relations_vals:
                        relations_vals.append(nm_table)
        return relations_vals

    def prepare_rel_dict(self):
        rettich = {}
        for rel in self._relations:
            rettich[rel.info] = rel
        return rettich

    def add_columns_to_relations(self, mapping_dict: dict):
        for relation in self._relations:
            if str(relation.info) in mapping_dict:
                relation.parse_column_dict(mapping_dict[str(relation.info)])
            else:
                raise MalformedMappingException(
                    "The mapping file does not contain mapping information on " + str(relation.info))

    def create_schema_script(self) -> str:
        creation_stmt_inner = ""
        unique_schemas = []
        for relation in self._relations:
            if relation.info.schema not in unique_schemas:
                unique_schemas.append(relation.info.schema)
        for schema in unique_schemas:
            creation_stmt_inner = creation_stmt_inner + f'CREATE SCHEMA IF NOT EXISTS "{schema}";\n\n'
        return creation_stmt_inner

    def make_creation_script(self) -> str:
        creation_stmt_inner = self.create_schema_script()
        done = []
        for relation in self._relations:
            if "n:1" not in relation.relations:
                creation_stmt_inner = creation_stmt_inner + relation.make_creation_script(self._rel_dict)
                done.append(relation)
        while len(done) != len(self._relations):
            progress = 0
            for relation in self._relations:
                if "n:1" in relation.relations:
                    brrr = False
                    for kek in relation.relations["n:1"]:
                        if kek not in done:
                            brrr = True
                    if not brrr:
                        creation_stmt_inner = creation_stmt_inner + relation.make_creation_script(self._rel_dict)
                        done.append(relation)
                        progress += 1
            if progress == 0:
                raise MalformedMappingException("There are cycles in the n:1 relations and therefore "
                                                "no foreign key could be generated.")
        return creation_stmt_inner


if __name__ == "__main__":
    relation_builder = RelationBuilder()
    with open("configurations/relations.json") as relation_file:
        relations = relation_builder.calculate_relations(json.load(relation_file))
    table_builder = TableBuilder(relations, "configurations/mappings.json")
    creation_stmt = table_builder.make_creation_script()
    pass
