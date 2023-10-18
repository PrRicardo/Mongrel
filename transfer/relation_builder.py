import json
from relation import Relation, RelationInfo


class RelationBuilder:
    @staticmethod
    def walk_paths(json_tracks: dict) -> list:
        if isinstance(json_tracks, dict):
            for key, value in json_tracks.items():
                if len(value) == 0:
                    yield [key]
                for p in RelationBuilder.walk_paths(value):
                    ret = [key]
                    ret.extend(p)
                    yield ret

    @staticmethod
    def get_relation_lists(json_tracks: dict) -> list:
        relation_lists = []
        for pathee in RelationBuilder.walk_paths(json_tracks):
            relation_lists.append(pathee)
        return relation_lists

    @staticmethod
    def fetch_unique_relations(relation_lists):
        relations = []
        for relation_list in relation_lists:
            for idx, val in enumerate(relation_list):
                if idx % 2 == 0:
                    if RelationInfo(val) not in relations:
                        relations.append(Relation(RelationInfo(val)))
        return relations

    @staticmethod
    def calculate_relations(json_tracks: dict):
        relation_lists = RelationBuilder.get_relation_lists(json_tracks)
        unique_relations = RelationBuilder.fetch_unique_relations(relation_lists)
        for relation in unique_relations:
            for relation_list in relation_lists:
                if str(relation.info) in relation_list:
                    parsed_relations = RelationBuilder.parse_relations(relation.info, relation_list)
                    relation.add_relations(parsed_relations)
        return unique_relations

    @staticmethod
    def parse_relations(info: RelationInfo, relation_list: list[str]) -> list[str, str]:
        def parse_left(location, lis):
            rel = lis[location - 1]
            rel_arr = rel.split(":")
            inverted = rel_arr[-1] + ':' + rel_arr[0]
            return inverted, lis[location - 2]

        def parse_right(location, lis):
            rel = lis[location + 1]
            return rel, lis[location + 2]

        relation = []
        idx = relation_list.index(str(info))
        if idx > 1:
            parsed_inv, parsed_val = parse_left(idx, relation_list)
            relation.append((str(parsed_inv), str(parsed_val)))
        if idx < len(relation_list) - 2:
            parsed_rel, parsed_val = parse_right(idx, relation_list)
            relation.append((str(parsed_rel), str(parsed_val)))
        return relation


if __name__ == '__main__':
    builder = RelationBuilder()
    with open("configurations/relations.json") as relation_file:
        relations = builder.calculate_relations(json.load(relation_file))
