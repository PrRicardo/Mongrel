"""
This module is used to provide a method to transfer data from mongodb to postgres!
"""

from .objects.transferrer import transfer_data_from_mongo_to_postgres
from .relation_discovery.configuration_builder import ConfigurationBuilder
