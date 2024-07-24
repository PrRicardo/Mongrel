from ...helpers.types.relation_type import RelationType


class Relation:
    left: str
    right: str
    relation_type: RelationType

    def __init__(self, left: str, right: str, relation_type: RelationType):
        self.left = left
        self.right = right
        self.relation_type = relation_type

    def __eq__(self, other):
        if not isinstance(other, Relation):
            return False
        return self.left == other.left and self.right == other.right
