import os
import pymongo
from mongrel_transferrer import ConfigurationBuilder
from mongrel_transferrer import transfer_data_from_mongo_to_postgres

if __name__ == "__main__":
    client = pymongo.MongoClient('localhost', 27017)
    mappings, relations = ConfigurationBuilder.build_configuration(
        client["hierarchical_relational_test"]["test_tracks"], cutoff=1.0, schema_name="relationizing")
    transfer_data_from_mongo_to_postgres(relations,
                                         mappings, mongo_host="localhost",
                                         mongo_database="hierarchical_relational_test",
                                         mongo_collection="test_tracks",
                                         sql_host='127.0.0.1', sql_database='spotify', sql_user='postgres',
                                         sql_port=35433, sql_password=os.getenv("PASSWORD"), batch_size=5000,
                                         keep_list_alias_relations=False)
