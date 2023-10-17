import json


class RelationInfo:
    table: str
    schema: str

    def __init__(self, table, schema):
        self.table = table
        self.schema = schema

class Relation:
    info: RelationInfo
    relations: dict[RelationInfo,str]
    def __init__(self,info):
        self.info = info


class RelationBuilder:
    def find_search_paths(self, relation_config:dict, paths:list[list[str]] = [[]]):
        layer = relation_config
        res = []
        for path in paths:
            for key,val in relation_config.items():
                inner_path = list(path)
                inner_path.append(key)
                res.append(inner_path)
        # TODO make sense here
        pass

if __name__ =='__main__':
    builder = RelationBuilder()
    with open("configurations/relations.json") as relation_file:
        builder.find_search_paths(json.load(relation_file))


