import json
import pymongo
from .map_flattener import flatten


class ConfigurationBuilder:

    @staticmethod
    def _make_name(path: str) -> str:
        """
        Make the name of the new field
        :param path: the entire path
        :return: the shortened path
        """
        name_arr = path.split('.')
        if len(name_arr) >= 2:
            return name_arr[-2] + '_' + name_arr[-1]
        return name_arr[-1]

    @staticmethod
    def _guess_type(value) -> str:
        """
        Type guessing function based on viewing a single value
        :param value: The value to look at
        :return: the inferred data type for the auto config
        """
        if isinstance(value, bool):
            return "BOOL"
        if isinstance(value, int):
            return "INTEGER"
        if isinstance(value, float):
            return "FLOAT"
        return "CHARACTER VARYING (1023)"

    @staticmethod
    def build_configuration(relation_config_path: str, mapping_config_path: str, mongo_host: str,
                            mongo_database: str, mongo_collection: str,
                            mongo_port: int = None, mongo_user: str = None,
                            mongo_password: str = None):
        """
        This method automatically generate the two required configuration files
        :param relation_config_path: path to the relation config file
        :param mapping_config_path: path to the mapping config file
        :param mongo_host: the ip address or name of the mongo server
        :param mongo_database: the database name of the source mongo
        :param mongo_collection: the collection that stores the source documents
        :param mongo_port: optional, the port of the source mongo database
        :param mongo_user: optional, the user of the source mongo database
        :param mongo_password: optional, the password of the user for the source database
        :return: Nothing, the files get saved to the given location
        """
        mongo_client = pymongo.MongoClient(host=mongo_host, port=mongo_port, username=mongo_user,
                                           password=mongo_password)
        db = mongo_client[mongo_database]
        collie = db[mongo_collection]
        try:
            example_document = collie.find()[0]
        except Exception as exc:
            raise FileExistsError(f"There was a issue getting an example document! "
                                  f"Please check the collection {mongo_collection}") from exc
        example_document = flatten(example_document)[0]
        configuration = {}
        for key, value in example_document.items():
            if key not in configuration:
                name = ConfigurationBuilder._make_name(key)
                typ = ConfigurationBuilder._guess_type(value)
                configuration[key] = name + " " + typ
        configuration = {mongo_collection: configuration}
        with open(mapping_config_path, "w", encoding="utf8") as file:
            configuration_str = json.dumps(configuration, indent=4)
            file.write(configuration_str)
        with open(relation_config_path, "w", encoding="utf8") as file:
            json.dump({mongo_collection: {}}, file)


if __name__ == "__main__":
    ConfigurationBuilder.build_configuration("relations.json",
                                             "mappings.json", mongo_host="localhost",
                                             mongo_database="hierarchical_relational_test",
                                             mongo_collection="test_tracks")
