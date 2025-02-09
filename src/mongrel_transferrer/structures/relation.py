from ..helpers.relation_types import RelationType


class Relation:
    type:RelationType
    _left_path:str
    _right_path:str
    _elements:set

    def __init__(self, left_path, right_path):
        self._left_path = left_path
        self._right_path = right_path
        self._elements = {str(left_path), str(right_path)}
        self.type = RelationType.one_to_one

    def __contains__(self, item:list[str]):
        return str(item) in self._elements

    def get_left(self):
        return self._left_path

    def get_right(self):
        return self._right_path