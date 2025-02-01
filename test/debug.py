import hashlib
import time

import pymongo
import json
from src.mongrel_transferrer.mongrel import MongrelTransferrer

from docker_tester import DockerManagement


class DebugDatabases:
    db_password: str
    mongo_port: int
    postgres_port: int
    _postgres_container_name: str
    _mongo_container_name: str

    def __init__(self, mongo_port: int = 27017, postgres_port: int = 5432):
        self._network_name = "uwu"
        self.db_password = hashlib.sha256(str(time.thread_time()).encode()).hexdigest()[:16]
        self.mongo_port = mongo_port
        self.postgres_port = postgres_port

    def create_infrastructure(self, pg_name: str = "HihiPostgreSQL", mg_name: str = "MongoNuzzlesOwO"):
        self._postgres_container_name = pg_name
        self._mongo_container_name = mg_name
        try:
            self.remove_infrastructure()
        except RuntimeError as e:
            pass
        DockerManagement.create_mongodb_server(network_name=mg_name, container_name=mg_name,
                                               db_port=self.mongo_port, db_password=self.db_password)
        DockerManagement.create_postgres_server(network_name=pg_name, container_name=pg_name,
                                                db_port=self.postgres_port, db_password=self.db_password)

    def write_test_mongo(self, db_name: str = "testing", db_collection: str = "collie"):
        with open('playlists.json', encoding="UTF-8") as file:
            json_data = json.load(file)
        client = pymongo.MongoClient(host="localhost", port=self.mongo_port, password=self.db_password,
                                     username="mongo", authSource="admin")
        db = client[db_name]
        collie = db[db_collection]
        for entry in json_data:
            collie.insert_one(entry)

    def remove_infrastructure(self):
        try:
            DockerManagement.remove_database_server(self._postgres_container_name, self._postgres_container_name)
        except:
            pass
        try:
            DockerManagement.remove_database_server(self._mongo_container_name, self._mongo_container_name)
        except:
            pass

    def __enter__(self):
        self.create_infrastructure()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.remove_infrastructure()


if __name__ == "__main__":
    with DebugDatabases(mongo_port=37030, postgres_port=35450) as debug_db:
        debug_db.write_test_mongo()
        transferrer = MongrelTransferrer(mongo_host="localhost", mongo_port=debug_db.mongo_port,
                                         mongo_user="mongo", mongo_password=debug_db.db_password,
                                         mongo_database="testing", mongo_collection="collie")
        transferrer.transfer()
