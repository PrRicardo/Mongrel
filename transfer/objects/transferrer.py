import os

from transfer.helpers.MapFlattener import flatten
from transfer.helpers.constants import Constants
from transfer.helpers.exceptions import MalformedMappingException
from transfer.objects.relation import Relation, RelationInfo, Field
from transfer.objects.relation_builder import RelationBuilder
from transfer.objects.table_builder import TableBuilder
from transfer.helpers.database_functions import DatabaseFunctions
from sqlalchemy import create_engine, URL, text
import json
import pandas as pd
import pymongo
from tqdm import tqdm


class Transferrer:
    dependencies: dict[RelationInfo, list[RelationInfo]]
    batch_size: int
    relations: list[Relation]
    structure_cache: dict[RelationInfo, dict]
    length_lookup: dict[RelationInfo, int]

    def __init__(self, relation_list: list[Relation], mongo_host: str, mongo_database: str, mongo_collection: str,
                 sql_host: str, sql_database: str, mongo_port: int = None, sql_port: int = None, sql_user=None,
                 sql_password=None, mongo_user: str = None, mongo_password: str = None, batch_size=100):

        self.mongo_collection = mongo_collection
        self.sql_password = sql_password
        self.sql_user = sql_user
        self.sql_port = sql_port
        self.mongo_password = mongo_password
        self.mongo_port = mongo_port
        self.sql_host = sql_host
        self.sql_database = sql_database
        self.mongo_user = mongo_user
        self.mongo_database = mongo_database
        self.mongo_host = mongo_host
        self.dependencies = Transferrer.create_dependencies(relation_list)
        self.relations = relation_list
        self.batch_size = batch_size
        self.structure_cache = dict()
        self.length_lookup = dict()

    def prepare_database(self, creation_script: str):
        url_object = URL.create("postgresql", username=self.sql_user, password=self.sql_password, host=self.sql_host,
                                port=self.sql_port, database=self.sql_database)
        engine_go_brr = create_engine(url_object)
        with engine_go_brr.connect() as connie:
            splitted = creation_script.split(";")
            for statement in splitted:
                statement = statement.strip()
                if len(statement) > 1:
                    connie.execute(text(statement))
                    connie.commit()

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

    def create_data_dict(self):
        data: dict[RelationInfo, pd.DataFrame] = {}
        for relation in self.relations:
            data[relation.info] = relation.make_df()
        return data

    def analyze_structure(self, doc: dict, relation: Relation):
        ret_set = dict()
        for column in relation.columns:
            val = doc
            prev_lists = []
            if column.path is not None:
                for step in column.path:
                    val = val[step]
                    if isinstance(val, list):
                        reeeeeee = []
                        reeeeeee.extend(prev_lists)
                        ret_set[step] = reeeeeee
                        prev_lists.append(step)
                        val = val[0]
        return ret_set

    def filter_dict(self, doc, relation: Relation, layer=0):
        if isinstance(doc, dict):
            rel_dict = {}
            for col in relation.columns:
                if col.path is not None:
                    for key in doc.keys():
                        if len(col.path) > layer and key == col.path[layer]:
                            rel_dict[key] = self.filter_dict(doc[key], relation, layer + 1)
            return rel_dict
        elif isinstance(doc, list):
            vals = []
            for entry in doc:
                vals.append(self.filter_dict(entry, relation, layer))
            return vals
        else:
            return doc

    def read_document_lines(self, doc: dict, relation: Relation) -> dict:
        if relation.info not in self.structure_cache:
            self.structure_cache[relation.info] = self.analyze_structure(doc, relation)
        return_values = dict()
        rel_dict = self.filter_dict(doc, relation)
        flattened = flatten(rel_dict, path_separator=Constants.PATH_SEP)
        for col in relation.columns:
            if col.path:
                if col.data_type.name != "PRIMARY_KEY":
                    return_values[col.target_name] = [col.conversion_function(val[col.translated_path],
                                                                              **col.conversion_args)
                                                      if col.translated_path in val else None
                                                      for val in flattened]
                else:
                    return_values[col.target_name] = []
                    for val in flattened:
                        if col.translated_path in val and val[col.translated_path] is not None:
                            return_values[col.target_name].append(col.conversion_function(val[col.translated_path],
                                                                                          **col.conversion_args))
                        else:
                            return_values[col.target_name].append(" ")
        return return_values

    def write_cascading(self, relation_info: RelationInfo, data: dict[RelationInfo, pd.DataFrame], connection):
        if relation_info in self.dependencies:
            for info in self.dependencies[relation_info]:
                self.write_cascading(info, data, connection)
        if len(data[relation_info]) > 0:
            data[relation_info].to_sql(name=relation_info.table, schema=relation_info.schema, if_exists="append",
                                       method=DatabaseFunctions.insert_on_conflict_nothing, con=connection, index=False)
            data[relation_info] = pd.DataFrame(columns=data[relation_info].columns)

    def transfer_data(self):
        mongo_client = pymongo.MongoClient(host=self.mongo_host, port=self.mongo_port, username=self.mongo_user,
                                           password=self.mongo_password)
        db = mongo_client[self.mongo_database]
        collie = db[self.mongo_collection]
        data: dict[RelationInfo, pd.DataFrame] = self.create_data_dict()
        url_object = URL.create("postgresql", username=self.sql_user, password=self.sql_password, host=self.sql_host,
                                port=self.sql_port, database=self.sql_database)
        engine_go_brr = create_engine(url_object)
        with engine_go_brr.connect() as connie:
            for doc in tqdm(collie.find()):
                for relation in self.relations:
                    vals = self.read_document_lines(doc, relation)
                    df = pd.DataFrame.from_dict(vals, orient='index').transpose()
                    data[relation.info] = pd.concat([data[relation.info], df], ignore_index=True)
                    if len(data[relation.info]) > self.batch_size:
                        self.write_cascading(relation.info, data, connie)
            for relation in self.relations:
                self.write_cascading(relation.info, data, connie)


if __name__ == "__main__":
    relation_builder = RelationBuilder()
    with open("../configurations/relations.json") as relation_file:
        relations = relation_builder.calculate_relations(json.load(relation_file))
    table_builder = TableBuilder(relations, "../configurations/mappings.json")
    creation_stmt = table_builder.make_creation_script()
    transferrer = Transferrer(table_builder.get_relations(), mongo_host="localhost", mongo_port=27017,
                              mongo_database="hierarchical_relational_test",
                              mongo_collection="test_tracks", sql_host='127.0.0.1', sql_database='ricardo',
                              sql_user='ricardo', sql_port=5432, sql_password=os.getenv("PASSWORD"))
    transferrer.prepare_database(creation_stmt)
    transferrer.transfer_data()
