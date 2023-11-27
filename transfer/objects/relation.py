"""
This file contains the main logic for the objects in the transfers.
"""
import pandas as pd
from transfer.helpers.constants import Constants
from transfer.helpers.conversions import Conversions
from transfer.helpers.exceptions import MalformedMappingException
from enum import Enum


class Field(Enum):
    """
    This enum is used to identify the three different types of fields in sql tables.
    """
    PRIMARY_KEY = 0
    FOREIGN_KEY = 1
    BASE = 2


class RelationInfo:
    """
    Relation Info objects describe the name of the relations used. They can have a schema and always need a table_name.
    """
    table: str
    schema: str

    def __init__(self, table: str, schema: str = None):
        """
        Parse a string into the table and schema format used in the relational database
        :param table: the table name or an entire string with schema.table
        :param schema: the schema name if it's already parsed somehow
        """
        # Check for schema if empty
        if schema is None:
            liz = table.split('.')
            if len(liz) > 1:
                self.table = liz[-1]
                self.schema = liz[-2]
            else:
                self.table = table
                self.schema = ''
        else:
            self.table = table
            self.schema = schema

    def __eq__(self, other):
        """
        Overloaded to make it a little bit easier to compare
        """
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

    def __hash__(self):
        """
        Implemented for the dictionary usage of RelationInfo
        """
        return hash(self.schema + ('.' if len(self.schema) > 0 else '') + self.table)


class Column:
    """
    The column class is used to represent columns in the tables which are used for the transfer configuration
    """
    target_name: str
    path: list[str]
    translated_path: str
    sql_definition: str
    field_type: Field
    foreign_reference: RelationInfo | None
    conversion_args: dict

    def __init__(self, target_name: str, path: list[str], sql_definition: str, field_type: Field,
                 foreign_reference: RelationInfo = None, conversion_function=None,
                 conversion_args=None):
        """
        Initalization of the column
            example in 
        :param target_name: name the column should get
        :param path: the path the json needs to be walked in, in order to reach the target value
        :param sql_definition: The sql data type definition that is going to be executed on the database
        :param field_type: Describes if the column is pk, fk or none of those
        :param foreign_reference: The reference of the column to another table using a relationInfo
        :param conversion_function: the conversion function that's going to be applied to every value read for that
        field
        :param conversion_args: Arguments the conversion function is being called with
        """
        self.target_name = target_name
        self.path = path
        self.sql_definition = sql_definition
        self.field_type = field_type
        self.foreign_reference = foreign_reference
        self.conversion_function = conversion_function if conversion_function else Conversions.do_nothing
        self.conversion_args = conversion_args if conversion_args else {}
        self.translated_path = ''
        if path is not None:
            for sub_path in path:
                self.translated_path += sub_path + Constants.PATH_SEP
            self.translated_path = self.translated_path[:-1]

    def __eq__(self, other):
        if isinstance(other, Column):
            if self.target_name == other.target_name and self.sql_definition == other.sql_definition:
                return True
        if isinstance(other, str):
            if self.target_name == other:
                return True
        return False

    def __hash__(self):
        return hash(self.target_name + str(self.path) + self.sql_definition + str(self.field_type))


class Relation:
    info: RelationInfo
    relations: dict[str, list[RelationInfo]]
    columns: list[Column]
    prepped: bool
    alias: RelationInfo | None

    def __init__(self, info, options: dict = None):
        self.info = info
        self.relations = {}
        self.columns = []
        self.prepped = False
        if options is None:
            options = {}
        self.alias = RelationInfo(options[Constants.ALIAS]) if Constants.ALIAS in options else None

    def __eq__(self, other):
        if isinstance(other, RelationInfo):
            if other == self.info:
                return True
        if isinstance(other, Relation):
            if other.info == self.info and self.relations == other.relations:
                return True
        return False

    def get_longest_path(self):
        lengths = []
        for column in self.columns:
            lengths.append(len(column.path))
        return max(lengths)

    def make_df(self) -> pd.DataFrame:
        collie_strs = []
        for col in self.columns:
            if col.path is not None:
                collie_strs.append(col.target_name)
        df = pd.DataFrame(columns=collie_strs)
        return df

    def add_relations(self, relation_list: list[tuple[str, str]]):
        for key, val in relation_list:
            if key in self.relations:
                if val not in self.relations[key]:
                    self.relations[key].append(RelationInfo(val))
            else:
                self.relations[key] = [RelationInfo(val)]

    def parse_column_dict(self, rel_dict):
        for key, value in rel_dict.items():
            if key != Constants.TRAN_OPTIONS:
                if Constants.TRAN_OPTIONS in rel_dict:
                    options = rel_dict[Constants.TRAN_OPTIONS]
                    self.add_column(key, value,
                                    options[Constants.REFERENCE_KEY] if Constants.REFERENCE_KEY in options else None,
                                    options[
                                        Constants.CONVERSION_FIELDS] if Constants.CONVERSION_FIELDS in options else None)
                else:
                    self.add_column(key, value)

    def prepare_columns(self, other_relations: dict, fk_are_pk=False):
        if "n:1" in self.relations and not self.prepped:
            for rel in self.relations["n:1"]:
                for other_col in other_relations[rel].columns:
                    rel_info = rel
                    if other_relations[rel].alias:
                        rel_info = other_relations[rel].alias
                    if other_col.field_type == Field.PRIMARY_KEY \
                            and f'{rel_info.table}_{other_col.target_name}' not in self.columns:
                        self.columns.append(
                            Column(f'{rel_info.table}_{other_col.target_name}', other_col.path,
                                   other_col.sql_definition,
                                   Field.PRIMARY_KEY if fk_are_pk else Field.FOREIGN_KEY, rel_info))
        self.prepped = True

    def get_alias_relations(self, other_relations: list) -> list | None:
        if not self.alias:
            return None
        return [kek for kek in other_relations if
                kek.alias == self.alias and kek != self]

    def make_creation_script(self, other_relations: dict, alias_relations: list = None):
        if not self.prepped:
            self.prepare_columns(other_relations)
        pk_count = 0
        schema_name = self.alias.schema if self.alias else self.info.schema
        table_name = self.alias.table if self.alias else self.info.table
        creation_stmt = f"CREATE TABLE IF NOT EXISTS "
        creation_stmt += f'"{schema_name}".' if len(schema_name) else ""
        creation_stmt += f'"{table_name}"(\n'
        for col in self.columns:
            creation_stmt += f'\t"{col.target_name}" {col.sql_definition},\n'
            pk_count += 1 if col.field_type == Field.PRIMARY_KEY else 0
        if alias_relations:
            for alias_relation in alias_relations:
                for col in alias_relation.columns:
                    if col not in self.columns:
                        creation_stmt += f'\t"{col.target_name}" {col.sql_definition},\n'
                        pk_count += 1 if col.field_type == Field.PRIMARY_KEY else 0
        if pk_count > 0:
            creation_stmt += "\tPRIMARY KEY("
            for col in self.columns:
                if col.field_type == Field.PRIMARY_KEY:
                    creation_stmt += f'"{col.target_name}", '
            if alias_relations:
                for alias_relation in alias_relations:
                    for col in alias_relation.columns:
                        if col not in self.columns:
                            if col.field_type == Field.PRIMARY_KEY:
                                creation_stmt += f'"{col.target_name}", '
            creation_stmt = creation_stmt[:-2]
            creation_stmt += "),\n"
        if "n:1" in self.relations:
            for rel in self.relations["n:1"]:
                appendix = other_relations[rel].alias.table if other_relations[rel].alias else rel.table
                creation_stmt += Relation.make_foreign_key(other_relations[rel], f'{appendix}_')
        if alias_relations:
            for alias_relation in alias_relations:
                if "n:1" in alias_relation.relations:
                    for rel in alias_relation.relations["n:1"]:
                        appendix = other_relations[rel].alias.table if other_relations[rel].alias else rel.table
                        creation_stmt += Relation.make_foreign_key(other_relations[rel], f'{appendix}_')
        creation_stmt = creation_stmt[:-2]
        creation_stmt += '\n);\n\n'
        return creation_stmt

    @staticmethod
    def make_foreign_key(relation, appendix=""):
        relation_schema = relation.alias.schema if relation.alias else relation.info.schema
        relation_table = relation.alias.table if relation.alias else relation.info.table
        creation_stmt = f'\tFOREIGN KEY ('
        for col in relation.columns:
            if col.field_type == Field.PRIMARY_KEY:
                creation_stmt += f'"{appendix}{col.target_name}",'
        creation_stmt = creation_stmt[:-1]
        creation_stmt += f") REFERENCES "
        creation_stmt += f'"{relation_schema}".' if len(relation_schema) else ""
        creation_stmt += f'"{relation_table}" ('
        for col in relation.columns:
            if col.field_type == Field.PRIMARY_KEY:
                creation_stmt += f'"{col.target_name}",'
        creation_stmt = creation_stmt[:-1]
        creation_stmt += f"),\n"
        return creation_stmt

    def create_nm_table(self, other, other_relations: dict):
        assert isinstance(other, Relation), "Something went wrong when creating a nm table"
        own_name = self.info.table if not self.alias else self.alias.table
        other_name = other.info.table if not other.alias else other.alias.table
        return_relation = Relation(RelationInfo(schema=self.info.schema, table=f'{own_name}2{other_name}'))
        return_relation.relations["n:1"] = [self.info, other.info]
        return_relation.prepare_columns(other_relations, fk_are_pk=True)
        return return_relation

    def handle_conversion_fields(self, conversion_field_dict: dict):
        pass

    def add_column(self, json_path: str, column_value: str, keys_dict: dict = None, convert_dict: dict = None):
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
                pass
                # FEATURE possible expansion to allow foreign keys, Currently not used
                # return Field.FOREIGN_KEY, RelationInfo(key_val.removeprefix("FK").strip())
            return Field.BASE, None

        def parse_column_conversion(convert_dict: dict):
            if Constants.SOURCE_TYPE not in convert_dict:
                raise MalformedMappingException(
                    f"{Constants.SOURCE_TYPE} not found in conversion definition {convert_dict}")
            if Constants.TARGET_TYPE not in convert_dict:
                raise MalformedMappingException(
                    f"{Constants.TARGET_TYPE} not found in conversion definition {convert_dict}")
            source_type = convert_dict[Constants.SOURCE_TYPE]
            target_type = convert_dict[Constants.TARGET_TYPE]
            conversion_function = Conversions.get_conversion(source_type, target_type)
            return conversion_function, convert_dict[
                Constants.CONV_ARGS] if Constants.CONV_ARGS in convert_dict else None

        name, definition = parse_column_value(column_value)
        if name not in self.columns:
            if keys_dict is not None and name in keys_dict:
                field_type, foreign_reference = parse_column_references(keys_dict[name])
            else:
                field_type, foreign_reference = Field.BASE, None
            if convert_dict is not None and name in convert_dict:
                conversion_function, extra_args = parse_column_conversion(convert_dict[name])
            else:
                conversion_function, extra_args = None, None
            broken_path = json_path.strip().split('.')
            self.columns.append(
                Column(name, broken_path, definition, field_type, foreign_reference, conversion_function, extra_args))
