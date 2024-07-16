from .relation_discovery import RelationDiscovery
import pymongo


class ConfigurationBuilder():
    @staticmethod
    def build_configuration(collection: pymongo.collection.Collection, schema_name: str = "public", cutoff=1.0) -> dict:
        relations = RelationDiscovery.calculate_relations(collection, cutoff)

