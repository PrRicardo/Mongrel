"""
Example usage of the transferer applied to the Spotify use case
"""

import os
from mongrel import transfer_data_from_mongo_to_postgres

if __name__ == "__main__":
    transfer_data_from_mongo_to_postgres("spotify_relations.json",
                                         "spotify_mappings.json", mongo_host="localhost",
                                         mongo_database="hierarchical_relational_test", mongo_collection="test_tracks",
                                         sql_host='127.0.0.1', sql_database='spotify', sql_user='postgres',
                                         sql_port=5432, sql_password=os.getenv("PASSWORD"), conflict_handling="Drop")
