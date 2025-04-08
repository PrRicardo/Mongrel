from src.mongrel_transferrer.structures.element_structure import ElementStructure


class TableStructure:
    base_name: str
    aliases: set[str]
    columns: set[str]
    sources: list[ElementStructure]

    def __init__(self, element: ElementStructure):
        self.base_name = element.identifier
        self.columns = element.columns
        self.sources = [element]
        self.aliases = set()

    def add_element(self, element: ElementStructure):
        if element in self.sources:
            return
        self.columns = self.columns | element.columns
        self.aliases.add(element.identifier)

    def shortest_alias(self):
        return min(self.aliases, key=len)

    def unionize_database_tables(self):
        pass

    def column_similarity(self, element: ElementStructure) -> float:
        relative_difference = (len(self.columns ^ element.columns)
                               / max(len(self.columns), len(element.columns)))
        return 1 - relative_difference
