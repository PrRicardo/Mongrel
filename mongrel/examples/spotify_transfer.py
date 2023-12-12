"""
Example usage of the transferer applied to the Spotify use case
"""

import os
from mongrel import transfer_data_from_mongo_to_postgres

if __name__ == "__main__":
    transfer_data_from_mongo_to_postgres("../configurations/spotify_relations.json",
                                         "../configurations/spotify_mappings.json", mongo_host="localhost",
                                         mongo_database="hierarchical_relational_test", mongo_collection="test_tracks",
                                         sql_host='127.0.0.1', sql_database='ricardo', sql_user='ricardo',
                                         sql_port=5432, sql_password=os.getenv("PASSWORD"))
