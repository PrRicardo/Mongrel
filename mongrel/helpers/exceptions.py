class MalformedMappingException(Exception):
    """
    This Exception is called when there are issues within the configuration File.
    A configuration file should not contain any circular dependencies and information on all tables used
    """
    pass
