from transfer.helpers.exceptions import MalformedMappingException
from transfer.objects.relation import Relation, RelationInfo
from transfer.objects.relation_builder import RelationBuilder
from transfer.objects.table_builder import TableBuilder
import json
import pandas as pd


class Transferrer:
    ordered_relations: list[RelationInfo]
    dependencies: dict[RelationInfo, list[RelationInfo]]
    data: dict[RelationInfo, pd.DataFrame]
    batch_size: int

    def __init__(self, relation_list: list[Relation], batch_size=100):
        self.ordered_relations = Transferrer.order_relations(relation_list)
        self.dependencies = Transferrer.create_dependencies(relation_list)
        self.batch_size = batch_size

    def prepare_database(self, creation_script: str):
        pass

    @staticmethod
    def order_relations(relation_list: list[Relation]) -> list[RelationInfo]:
        done = []
        for relation in relation_list:
            if "n:1" not in relation.relations:
                done.append(relation.info)
        while len(done) != len(relation_list):
            progress = 0
            for relation in relation_list:
                if "n:1" in relation.relations:
                    brrr = False
                    for kek in relation.relations["n:1"]:
                        if kek not in done:
                            brrr = True
                    if not brrr:
                        done.append(relation.info)
                        progress += 1
            if progress == 0:
                raise MalformedMappingException("There are cycles in the n:1 relations and therefore "
                                                "no foreign key could be generated.")

        return done

    @staticmethod
    def create_dependencies(relation_list: list[Relation]) -> dict[RelationInfo, list[RelationInfo]]:
        dependencies: dict[RelationInfo, list[RelationInfo]] = {}
        for relation in relation_list:
            for column in relation.columns:
                if column.foreign_reference is not None:
                    if relation.info in dependencies:
                        dependencies[relation.info].append(column.foreign_reference)
                        # I don't know why but a duplicate check in the if-clause just does not work
                        dependencies[relation.info] = list(set(dependencies[relation.info]))
                    else:
                        dependencies[relation.info] = [column.foreign_reference]
        return dependencies

    def transfer_data(self):
        # while entries
        # get entries
        # write to dfs
        # write all
        pass


if __name__ == "__main__":
    relation_builder = RelationBuilder()
    with open("../configurations/relations.json") as relation_file:
        relations = relation_builder.calculate_relations(json.load(relation_file))
    table_builder = TableBuilder(relations)
    with open("../configurations/mappings.json") as relation_file:
        table_builder.add_columns_to_relations(json.load(relation_file))
    creation_stmt = table_builder.make_creation_script()
    transferrer = Transferrer(table_builder.relations)
    transferrer.prepare_database(creation_stmt)
