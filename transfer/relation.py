from transfer.constants import Constants
from transfer.exceptions import MalformedMappingException
from enum import Enum


class Field(Enum):
    PRIMARY_KEY = 0
    FOREIGN_KEY = 1
    BASE = 2


class RelationInfo:
    table: str
    schema: str

    def __init__(self, table: str, schema: str = None):
        if schema is None:
            liz = table.split('.')
            if len(liz) > 1:
                self.table = liz[-1]
                self.schema = liz[-2]
            else:
                self.table = table
                self.schema = ""
        else:
            self.table = table
            self.schema = schema

    def __eq__(self, other):
        if isinstance(other, RelationInfo):
            if other.table == self.table and other.schema == self.schema:
                return True
        else:
            parsed = str(other)
            splittie = parsed.split('.')
            if len(splittie) != 2:
                return False
            if splittie[1] == self.table and splittie[0] == self.schema:
                return True
        return False

    def __str__(self):
        return self.schema + ('.' if len(self.schema) > 0 else '') + self.table


class Column:
    target_name: str
    path: list[str]
    sql_definition: str
    data_type: Field
    foreign_reference: list[RelationInfo] | None = None

    def __init__(self, target_name: str, path: list[str], sql_definition: str, data_type: Field):
        self.target_name = target_name
        self.path = path
        self.sql_definition = sql_definition
        self.data_type = data_type

    def __eq__(self, other):
        if isinstance(other, Column):
            if self.target_name == other.target_name and self.path == other.path \
                    and self.sql_definition == other.sql_definition:
                return True
        if isinstance(other, str):
            if self.target_name == other:
                return True
        return False

    def set_foreign_reference(self, relation_info: RelationInfo):
        if self.data_type != Field.FOREIGN_KEY:
            raise MalformedMappingException("A column with a foreign key reference needs to be declared like a PK.")
        self.foreign_reference = relation_info


class Relation:
    info: RelationInfo
    relations: dict[str, list[RelationInfo]]
    columns: list[Column]

    def __init__(self, info):
        self.info = info
        self.relations = {}
        self.columns = []

    def __eq__(self, other):
        if isinstance(other, RelationInfo):
            if other == self.info:
                return True
        if isinstance(other, Relation):
            if other.info == self.info and self.relations == other.relations:
                return True
        return False

    def add_relations(self, relation_list: list[str, str]):
        for key, val in relation_list:
            if key in self.relations:
                if val not in self.relations[key]:
                    self.relations[key].append(RelationInfo(val))
            else:
                self.relations[key] = [RelationInfo(val)]

    def parse_column_dict(self, rel_dict):
        for key, value in rel_dict.items():
            if key != Constants.CONVERSION_FIELDS and key != Constants.REFERENCE_KEY:
                self.add_column(key, value,
                                rel_dict[Constants.REFERENCE_KEY] if Constants.REFERENCE_KEY in rel_dict else None)
        if Constants.CONVERSION_FIELDS in rel_dict:
            self.handle_conversion_fields(rel_dict[Constants.CONVERSION_FIELDS])

    def create_relevant_tables(self):
        creation_own = f"""
        """

    def handle_conversion_fields(self, conversion_field_dict: dict):
        pass

    def add_column(self, json_path: str, column_value: str, keys_dict: dict = None):
        def parse_column_value(col_val: str):
            col_val = col_val.strip()
            splittie = col_val.split(" ")
            if len(splittie) < 2:
                raise MalformedMappingException(f"{col_val} needs to be of the form 'name TYPE DEFINITION', "
                                                f"like in a create statement.")
            col_val = col_val.removeprefix(splittie[0])
            col_val = col_val.strip()
            return splittie[0], col_val

        def parse_column_references(key_val: str | dict):
            if isinstance(key_val, str) and key_val.strip().startswith('PK'):
                return Field.PRIMARY_KEY, None
            if isinstance(key_val, dict):
                # TODO possible expansion to allow foreign keys
                return Field.FOREIGN_KEY, RelationInfo(key_val.removeprefix("FK").strip())
            return Field.BASE, None

        name, definition = parse_column_value(column_value)
        if name not in self.columns:
            if keys_dict is not None and name in keys_dict:
                field_type, foreign_reference = parse_column_references(keys_dict[name])
            else:
                field_type, foreign_reference = Field.BASE, None
            broken_path = json_path.strip().split('.')
            self.columns.append(Column(name, broken_path, definition, field_type))
