from enum import Enum

class RelationType(Enum):
    one_to_one = 1
    one_to_many = 2
    many_to_many = 3