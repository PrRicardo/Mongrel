from src.mongrel_transferrer.structures.table_structure import TableStructure
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection


class MongrelTransferrer:
    """
    The main class that handles the transfer
    """
    tables: list[TableStructure]

    def __init__(self, mongo_host: str, mongo_database: str, mongo_collection: str,
                 sql_host: str=None, sql_database: str=None, mongo_port: int = None, sql_port: int = None, sql_user=None,
                 sql_password=None, mongo_user: str = None, mongo_password: str = None, batch_size=1000):
        """
        Initializes the transfer class with all the required information
        :param relation_list: the list of all prepped relations
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

    def add_doc(self, doc:dict):
        pass

    def transfer(self):
        with MongoClient(host=self.mongo_host, port=self.mongo_port, username=self.mongo_user,
                             password=self.mongo_password) as client:
            database:Database = client[self.mongo_database]
            collection:Collection = database[self.mongo_collection]
            for doc in collection.find():
                self.add_doc(doc)

