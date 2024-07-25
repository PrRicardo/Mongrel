"""
Example usage of the transferer applied to the Spotify use case
"""
import json
import os
from src.mongrel_transferrer.mongrel import transfer_data_from_mongo_to_postgres

if __name__ == "__main__":
    with open("spotify_relations.json") as relations:
        with open("spotify_mappings.json") as mappings:
            transfer_data_from_mongo_to_postgres(json.load(relations),
                                                 json.load(mappings), mongo_host="localhost",
                                                 mongo_database="hierarchical_relational_test",
                                                 mongo_collection="test_tracks",
                                                 sql_host='127.0.0.1', sql_database='spotify', sql_user='postgres',
                                                 sql_port=35433, sql_password=os.getenv("PASSWORD"))
