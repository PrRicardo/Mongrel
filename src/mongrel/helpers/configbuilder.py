import json
import pymongo
from .map_flattener import flatten


class ConfigurationBuilder:

    @staticmethod
    def _make_name(path: str) -> str:
        name_arr = path.split('.')
        if len(name_arr) >= 2:
            return name_arr[-2] + '_' + name_arr[-1]
        return name_arr[-1]

    @staticmethod
    def _guess_type(value) -> str:
        if isinstance(value, bool):
            return "BOOLEAN"
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
