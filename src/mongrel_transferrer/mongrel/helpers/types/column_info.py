from typing import Any
from ...helpers.types.bloom_filter import BloomFilter
from ...helpers.types.data_type import Datatype


class ColumnInfo:
    values: BloomFilter
    unique: bool
    is_table: bool
    is_list: bool
    path: list
    data_type: Datatype
    length: int

    def __init__(self, expected_values: int, is_table: bool = False, is_list: bool = False,
                 false_positive_acceptance: int = 0.000000001, path: list = None, datatype: Datatype = None,
                 length: int = None):
        self.path = path
        self.values = BloomFilter(expected_values, false_positive_acceptance)
        self.unique = True
        self.is_table = is_table
        self.is_list = is_list
        self.data_type = Datatype.BOOLEAN if not datatype else datatype
        self.length = 0 if not length else length

    def get_data_type(self) -> str:
        return self.data_type.name

    def calculate_type(self, value: Any) -> Datatype:
        if isinstance(value, bool):
            return Datatype.BOOLEAN
        if isinstance(value, int):
            return Datatype.INTEGER
        if isinstance(value, float):
            return Datatype.FLOAT
        if isinstance(value, str):
            return Datatype.TEXT
        return Datatype.NOT_ADAPTABLE

    def add_value(self, value) -> bool:
        data_type = self.calculate_type(value)
        if data_type.value > self.data_type.value:
            self.data_type = data_type
        if data_type == Datatype.TEXT:
            stringified_value = str(value)
            self.length = max(self.length, len(stringified_value))
        if value in self.values:
            self.unique = False
            return True
        self.values.add(value, checked=True)
        return False

    def __contains__(self, item):
        return item in self.values
