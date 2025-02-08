from typing import Self

from src.mongrel_transferrer.structures.relation import Relation


class ElementStructure:
    parent:Self
    children: list[Self]
    frequency_dist: set
    path:list[str]
    identifier:str
    relations:list[Relation]

    def __init__(self, parent:Self, path:list[str]):
        self.parent = parent
        self.children = []
        self.frequency_dist = set()
        self.path = path
        self.identifier = path[-1]
        self.relations = []

    def add_doc(self, sub_element:dict):
        raise NotImplementedError