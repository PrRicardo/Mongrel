import pymongo
from src.mongrel.relation_discovery.configuration_builder import ConfigurationBuilder

if __name__ == "__main__":
    client = pymongo.MongoClient('localhost', 27017)
    ConfigurationBuilder.build_configuration(client["hierarchical_relational_test"]["test_tracks"], cutoff=1.0)