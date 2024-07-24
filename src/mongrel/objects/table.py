"""
This file contains the main logic for the objects in the transfers.
"""
from enum import Enum
import pandas as pd
from ..helpers.constants import PATH_SEP, CONVERSION_FIELDS, ALIAS, CONV_ARGS, REFERENCE_KEY, TRAN_OPTIONS, \
    TARGET_TYPE, SOURCE_TYPE
from ..helpers.conversions import Conversions
from ..helpers.exceptions import MalformedMappingException
from ..helpers.utils import decide_sql_definition
from typing import Any


class Field(Enum):
    """
    This enum is used to identify the three different types of fields in sql tables.
    """
    PRIMARY_KEY = 0
    FOREIGN_KEY = 1
    BASE = 2


class TableInfo:
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
        if isinstance(other, TableInfo):
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
    foreign_reference: TableInfo
    conversion_args: dict
    conversion_function: Any

    def __init__(self, target_name: str, path: list[str], sql_definition: str, field_type: Field,
                 foreign_reference: TableInfo = None, conversion_function=None,
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
                self.translated_path += sub_path + PATH_SEP
            self.translated_path = self.translated_path[:-1]

    def __eq__(self, other):
        """
        Equality had to be overloaded for how it is used in the code
        A column object is the same if the target name and sql definitions are the same
        A string is the same if it is equal to the target name
        :param other: a colum or just a string
        :return: boolean value representing equality
        """
        if isinstance(other, Column):
            if self.target_name == other.target_name and self.sql_definition == other.sql_definition:
                return True
        if isinstance(other, str):
            if self.target_name == other:
                return True
        return False

    def __hash__(self):
        """
        Hashing implemented for usage in dictionaries
        :return: hashed targetname, path, sql_definition and fieldtype
        """
        return hash(self.target_name + str(self.path) + self.sql_definition + str(self.field_type))


class Table:
    """
    This class is representing the relations/tables. It contains information about the columns, existing relations to
    other tables, information on naming and aliases
    """
    info: TableInfo
    relations: dict[str, list[TableInfo]]
    columns: list[Column]
    prepped: bool
    alias: TableInfo
    pks: list[str]

    def __init__(self, info, options: dict = None):
        """
        Initalizes the class with empty relations and columns which get added later in the process
        :param info: RelationInfo of the table containing target naming information
        :param options: option dictionary in the mapping file
        """
        self.info = info
        self.relations = {}
        self.columns = []
        self.prepped = False
        if options is None:
            options = {}
        self.alias = TableInfo(options[ALIAS]) if ALIAS in options else None
        self.pks = []

    def __eq__(self, other):
        """
        Overloaded equality
        When compared with relationInfo object, equality when relationInfo is this object's info
        When compared with another relation, equality if the infos and relations to other tables are equal
        :param other: a RelationInfo or Relation object
        :return: Boolean representing equality
        """
        if isinstance(other, TableInfo):
            if other == self.info:
                return True
        if isinstance(other, Table):
            if other.info == self.info and self.relations == other.relations:
                return True
        return False

    def make_df(self) -> pd.DataFrame:
        """
        Creates a Dataframe object containing all the columns
        :return: The created Dataframe
        """
        collie_strs = []
        for col in self.columns:
            if col.path is not None:
                collie_strs.append(col.target_name)
        df = pd.DataFrame(columns=collie_strs)
        df.set_index(self.pks)
        return df

    def add_relations(self, relation_list: list[tuple[str, str]]) -> None:
        """
        Adds relations from mapping parsing to the relation object
        :param relation_list: a list containing relation type and target name of related objects
        """
        for key, val in relation_list:
            if key in self.relations:
                if val not in self.relations[key]:
                    self.relations[key].append(TableInfo(val))
            else:
                self.relations[key] = [TableInfo(val)]

    def parse_column_dict(self, rel_dict: dict) -> None:
        """
        Adds the columns to the class, parsing their conversion functions if necessary
        :param rel_dict: The dictionary of relevant column information
        """
        for key, value in rel_dict.items():
            if key != TRAN_OPTIONS:
                if TRAN_OPTIONS in rel_dict:
                    options = rel_dict[TRAN_OPTIONS]
                    self.add_column(key, value,
                                    options[REFERENCE_KEY] if REFERENCE_KEY in options else None,
                                    options[
                                        CONVERSION_FIELDS] if CONVERSION_FIELDS in options else None)
                else:
                    self.add_column(key, value)

    def prepare_columns(self, other_relations: dict, fk_are_pk: bool = False) -> None:
        """
        Prepares the columns for n:1 relations since they extend the columns of the relation.
        Sets the prepared status of the table to true.
        :param other_relations: a dictionary including information on all other dicts to look up column names and
        definitions
        :param fk_are_pk: if foreign keys are primary keys for this table -> required for m:n helper tables
        """
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
        for column in self.columns:
            if column.field_type == Field.PRIMARY_KEY and column.target_name not in self.pks:
                self.pks.append(column.target_name)
        self.prepped = True

    def get_alias_relations(self, other_relations: list) -> list:
        """
        Fetch all relations that have the same alias as this relation
        :param other_relations: a list containing all relation objects
        :return: a list of all other relations with alias == self.alias or None
        """
        if not self.alias:
            return []
        return [kek for kek in other_relations if
                kek.alias == self.alias and kek != self]

    def write_columns(self, alias_relations: list = None):
        """
        Writes all columns for the creation of the database
        :param alias_relations: alias relations if any exist
        :return: returns the creation stmt part of the columns
        """
        creation_stmt = ''
        creation_dict = {}
        for col in self.columns:
            if col.target_name not in creation_dict:
                creation_dict[col.target_name] = col.sql_definition
        if alias_relations:
            for alias_relation in alias_relations:
                for col in alias_relation.columns:
                    if col.target_name not in creation_dict:
                        creation_dict[col.target_name] = col.sql_definition
                    elif col.sql_definition != creation_dict[col.target_name]:
                        creation_dict[col.target_name] = decide_sql_definition(col.sql_definition,
                                                                               creation_dict[col.target_name]).upper()
        for target_name, sql_definition in creation_dict.items():
            creation_stmt += f'\t"{target_name}" {sql_definition},\n'
        return creation_stmt

    def write_primary_key(self, alias_relations: list = None):
        """
        Writes the primary key constraint of the creation stmt
        :param alias_relations: alias_relations if any exist
        :return: the creation stmt part that defines the primary key
        """
        creation_stmt = "\tPRIMARY KEY("
        pks = set()
        for col in self.columns:
            if col.field_type == Field.PRIMARY_KEY:
                pks.add(col.target_name)
        if alias_relations:
            for alias_relation in alias_relations:
                for col in alias_relation.columns:
                    if col not in self.columns:
                        if col.field_type == Field.PRIMARY_KEY:
                            pks.add(col.target_name)
        for pk in pks:
            creation_stmt += f'"{pk}", '
        creation_stmt = creation_stmt[:-2]
        creation_stmt += "),\n"
        return creation_stmt

    def write_foreign_keys(self, other_relations: dict, alias_relations: list = None):
        """
        Writes the foreign key constraint of the creation stmt based on the foreign relations
        :param other_relations: a dictionary containing information on all relations [RelationInfo, Relation]
        :param alias_relations: all relations that have the same alias as the relation
        :return: the part of the creation statement that defines the foreign keys
        """
        creation_stmt = ""
        if "n:1" in self.relations:
            for rel in self.relations["n:1"]:
                appendix = other_relations[rel].alias.table if other_relations[rel].alias else rel.table
                creation_stmt += Table.make_foreign_key(other_relations[rel], f'{appendix}_')
        if alias_relations:
            for alias_relation in alias_relations:
                if "n:1" in alias_relation.relations:
                    for rel in alias_relation.relations["n:1"]:
                        appendix = other_relations[rel].alias.table if other_relations[rel].alias else rel.table
                        creation_stmt += Table.make_foreign_key(other_relations[rel], f'{appendix}_')
        return creation_stmt

    def count_primary_key_fields(self, alias_relations: list = None):
        """
        Counts all primary key fields of the relation
        :param alias_relations: all relations that have the same alias as the relation
        :return: the number of primary key fields for this relation
        """
        pk_count = 0
        for col in self.columns:
            pk_count += 1 if col.field_type == Field.PRIMARY_KEY else 0
        if alias_relations:
            for alias_relation in alias_relations:
                for col in alias_relation.columns:
                    if col not in self.columns:
                        pk_count += 1 if col.field_type == Field.PRIMARY_KEY else 0
        return pk_count

    def make_creation_script(self, other_relations: dict, alias_relations: list = None) -> str:
        """
        Builds the creation statement for this table.
        :param other_relations: a dictionary containing all the other relations dict[RelationInfo,Relation]
        :param alias_relations: all relations that have the same alias as this relation
        :return: the creation script for this relation
        """
        if not self.prepped:
            self.prepare_columns(other_relations)
        if alias_relations:
            for alias_relation in alias_relations:
                if not alias_relation.prepped:
                    alias_relation.prepare_columns(other_relations)
        pk_count = self.count_primary_key_fields(alias_relations)
        schema_name = self.alias.schema if self.alias else self.info.schema
        table_name = self.alias.table if self.alias else self.info.table
        creation_stmt = "CREATE TABLE IF NOT EXISTS "
        creation_stmt += f'"{schema_name}".' if len(schema_name) else ""
        creation_stmt += f'"{table_name}"(\n'
        creation_stmt += self.write_columns(alias_relations)
        if pk_count > 0:
            creation_stmt += self.write_primary_key(alias_relations)
        creation_stmt += self.write_foreign_keys(other_relations, alias_relations)
        creation_stmt = creation_stmt[:-2]
        creation_stmt += '\n);\n\n'
        return creation_stmt

    @staticmethod
    def make_foreign_key(relation: object, appendix: str = ""):
        """
        writes a single foreign key
        :param relation: the relation that is used as the foreign key base
        :param appendix: the appendix for the columns of the other table
        :return: returns the single foreign key fo the relation
        """
        assert isinstance(relation, Table)
        relation_schema = relation.alias.schema if relation.alias else relation.info.schema
        relation_table = relation.alias.table if relation.alias else relation.info.table
        creation_stmt = '\tFOREIGN KEY ('
        for col in relation.columns:
            if col.field_type == Field.PRIMARY_KEY:
                creation_stmt += f'"{appendix}{col.target_name}",'
        creation_stmt = creation_stmt[:-1]
        creation_stmt += ") REFERENCES "
        creation_stmt += f'"{relation_schema}".' if len(relation_schema) else ""
        creation_stmt += f'"{relation_table}" ('
        for col in relation.columns:
            if col.field_type == Field.PRIMARY_KEY:
                creation_stmt += f'"{col.target_name}",'
        creation_stmt = creation_stmt[:-1]
        creation_stmt += "),\n"
        return creation_stmt

    def create_nm_table(self, other: object, other_relations: dict):
        """
        Creates the helper table for n:m relations
        :param other: the other table of the relation
        :param other_relations: all the other relations in a dict [RelationInfo, Relation]
        :return: returns the created helper relation that has n:1 relations to the other tables
        """
        assert isinstance(other, Table), "Something went wrong when creating a nm table"
        own_name = self.info.table if not self.alias else self.alias.table
        other_name = other.info.table if not other.alias else other.alias.table
        return_relation = Table(TableInfo(schema=self.info.schema, table=f'{own_name}2{other_name}'))
        return_relation.relations["n:1"] = [self.info, other.info]
        return_relation.prepare_columns(other_relations, fk_are_pk=True)
        return return_relation

    def add_column(self, json_path: str, column_value: str, keys_dict: dict = None, convert_dict: dict = None) -> None:
        """
        Adds a column to the relation if it does not yet exist, as well as foreign relations and required information
        for the relation class
        :param json_path: the json path to the value written in the config file
        :param column_value: the column information of the config file including name and sql definition
        :param keys_dict: the dictionary containing information about pks fks and so on
        :param convert_dict: the configuration information about conversion for this field
        """

        def parse_column_value(col_val: str):
            col_val = col_val.strip()
            splittie = col_val.split(" ")
            if len(splittie) < 2:
                raise MalformedMappingException(f"{col_val} needs to be of the form 'name TYPE DEFINITION', "
                                                f"like in a create statement.")
            col_val = col_val.removeprefix(splittie[0])
            col_val = col_val.strip()
            return splittie[0], col_val

        def parse_column_references(key_val):
            if isinstance(key_val, str) and key_val.strip().startswith('PK'):
                return Field.PRIMARY_KEY, None
            if isinstance(key_val, dict):
                pass
                # FEATURE possible expansion to allow foreign keys, Currently not used
                # return Field.FOREIGN_KEY, RelationInfo(key_val.removeprefix("FK").strip())
            return Field.BASE, None

        def parse_column_conversion(convert_dict: dict):
            if SOURCE_TYPE not in convert_dict:
                raise MalformedMappingException(
                    f"{SOURCE_TYPE} not found in conversion definition {convert_dict}")
            if TARGET_TYPE not in convert_dict:
                raise MalformedMappingException(
                    f"{TARGET_TYPE} not found in conversion definition {convert_dict}")
            source_type = convert_dict[SOURCE_TYPE]
            target_type = convert_dict[TARGET_TYPE]
            conversion_function = Conversions.get_conversion(source_type, target_type)
            return conversion_function, convert_dict[
                CONV_ARGS] if CONV_ARGS in convert_dict else None

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
