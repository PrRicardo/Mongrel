import sqlalchemy

from src.mongrel_transferrer.helpers.constants import ROOT_COLUMN
from src.mongrel_transferrer.structures.element_structure import ElementStructure
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection


def create_postgres_connection_string(host: str, dbname: str, user: str = None,
                                      password: str = None, port: int = 5432) -> str:
    connection_url = f"postgresql://"
    if user and password:
        connection_url += f"{user}:{password}@"
    connection_url += f"{host}:{port}/{dbname}"
    return connection_url

class MongrelTransferrer:
    """
    The main class that handles the transfer
    """
    root:ElementStructure

    def __init__(self, mongo_host: str, mongo_database: str, mongo_collection: str,
                 sql_host: str = None, sql_database: str = None, mongo_port: int = None,
                 sql_port: int = None, sql_user=None, sql_password=None, mongo_user: str = None,
                 mongo_password: str = None, batch_size=10000):
        """
        Initializes the transfer class with all the required information
        :param mongo_host: the ip address or name of the mongo server
        :param mongo_database: the database name of the source mongo
        :param mongo_collection: the collection that stores the source documents
        :param sql_host: the ip address or name of the postgres server
        :param sql_database: the name of the target postgres database
        :param mongo_port: optional, the port of the source mongo database
        :param sql_port: optional, the port of the target sql database
        :param sql_user: optional, the user of the target database
        :param sql_password: optional, the password of the user for the target database
        :param mongo_user: optional, the user of the source mongo database
        :param mongo_password: optional, the password of the user for the source database
        :param batch_size: the batch size used
        """
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
        self.batch_size = batch_size
        self.length_lookup = {}
        self.identifier_lookup = {ROOT_COLUMN}
        self.root = ElementStructure(ROOT_COLUMN, self.identifier_lookup)

    def add_doc(self, doc: dict):
        self.root.add_doc(doc)

    def transfer(self):
        first_write = True
        with MongoClient(host=self.mongo_host, port=self.mongo_port, username=self.mongo_user,
                         password=self.mongo_password) as client:
            database: Database = client[self.mongo_database]
            collection: Collection = database[self.mongo_collection]
            engine = sqlalchemy.create_engine(create_postgres_connection_string(
                host=self.sql_host,
                dbname=self.sql_database,
                user=self.sql_user,
                password=self.sql_password,
                port=self.sql_port
            ))
            for doc in collection.find():
                self.add_doc(doc)
                if self.root.largest_buffer_length() > self.batch_size:
                    self.root.write('public', engine=engine, replace=first_write)
                    first_write = False
            self.root.write('public', engine=engine, replace=first_write)
