"""
    MONGREL: MONgodb Going RELational
    Copyright (C) 2023 Ricardo Prida

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.

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
