"""
A lightweight, application-agnostic API to interface with a database.

Currently only supports SQLite

Allows creation/deletion/updates of entries as either database-internal storage,
or as accession IDs of information (ie, file paths in the configured '$MODULE/DB' directory)

Implementation-specific details are left to the calling module. Default configuration is not provided.
"""
import copy
import logging
import os
import sqlite3

from typing import NamedTuple

from .log_utilities import create_logger
from .string_utilities import string_to_camel_case

# TODO: Documentation!


def _execute_command(cur, command: str):

    try:
        cur.execute(command)
    # except sqlite3.IntegrityError as ie:
    #     raise ie
    except Exception as e:
        raise e


def _retrieve_data(cur, command: str):

    try:
        cur.execute(command)
        return cur.fetchall()
    except Exception as e:
        raise


def _config_tables_to_commands(tables: dict):

    # Return values
    commands = []

    for table, fields in tables.items():
        primary_key_defined = False
        for field_name, properties in fields.items():
            if primary_key_defined:
                break
            for field_definition in properties["definition"]:
                if "PRIMARY KEY" in field_definition:
                    primary_key_defined = True
                    break

        cmd = f'CREATE TABLE IF NOT EXISTS "{table}"'
        cmd += " (id integer PRIMARY KEY, " if not primary_key_defined else " ("

        cmd_suffix = ""

        for field_name in fields.keys():
            field_properties = fields[field_name]
            field_definition = field_properties["definition"]
            field_t = field_definition[0].upper()
            if field_t in SQLInterface.sqlite_types:
                cmd += f'"{field_name}" {field_t}'
                if len(field_definition) > 1:
                    cmd += " " + " ".join(field_definition[1:])
            else:
                raise TypeError(
                    f'Data type {field_t} (in field "{field_name}") not recognized as SQL type.'
                    f"Available types are {SQLInterface.sqlite_types.keys()}"
                )
            if "foreign key" in field_properties:
                fk_details = field_properties["foreign key"]
                cmd_suffix += ', FOREIGN KEY ("{}") REFERENCES "{}" ("{}")'.format(
                    field_name, fk_details[0], fk_details[1]
                )
            cmd += ", "

        cmd = cmd.rstrip(", ")
        if cmd_suffix:
            cmd += cmd_suffix
        if primary_key_defined:
            cmd += ") WITHOUT ROWID;"
        else:
            cmd += ");"
        commands.append(cmd)

    return commands


def validate_config(config: dict):
    try:
        assert {"path", "tables"}.issubset(
            config.keys()
        ), "Invalid configuration for SQLLite interface"
        tables = config["tables"]
        assert isinstance(
            tables, dict
        ), "Table creation definition must be a dictionary. See documentation"
        for fields in tables.values():
            assert isinstance(
                fields, dict
            ), "Field definitions for tables must be a list of dictionaries. See documentation"
    except Exception:
        raise ValueError("Invalid configuration. See stack trace for information")


class SQLInterface:

    sqlite_types = {
        "NULL": "",
        "INTEGER": int,
        "REAL": float,
        "TEXT": str,
        "BLOB": bytes,
    }

    def __init__(self, config: dict = None, log_name: str = None):
        """
        'config' should include the following information:
        'path': Base directory for where data should be stored
              For SQLite databases, this will be the root folder
              which contains the database and the 'DB' folder
              that contains linked information.
        'project': Project name for the database to inherit. Must be provided
                   in the configuration. Will yield the file '<project>.db'
        'tables': Definition of tables as key-value pairs, e.g.
              {'<table_name>': {'<field_name>', 'type: [<sqlite type> OR <ptr pointer/path>}']}
        """

        if log_name is not None:
            # Check if the logger has been created
            if log_name in logging.root.manager.loggerDict:
                self.logger = logging.getLogger(log_name)
            else:
                create_logger(log_name=log_name)
                self.logger = logging.getLogger(log_name)
                self.logger.info(
                    "Creating a logger with default configuration."
                    " The preferred use-case is to create the logger"
                    " prior to initializing this interface"
                )
            self.log_active = True
        else:
            self.logger = None
            self.log_active = False
        if self.logger:
            self.logger.debug("Validating configuration")
        validate_config(config)
        self.config = config

        db_name = "DB" if "project" not in config else (config["project"] + "_DB")
        base_path = os.path.abspath(os.path.expanduser(config["path"]))
        self.db_path = base_path
        self.db = base_path + db_name + ".db"
        if self.logger:
            self.logger.debug(f"Attempting to connect to database at path {self.db}")
        self.conn = sqlite3.connect(self.db)
        self.cur = self.conn.cursor()
        self.meta_info = {"tables": {}}

    def activate_logging(self):
        if self.logger:
            self.log_active = True
            self.logger.info("Activating logging facility")

    def deactivate_logging(self):
        if self.logger:
            self.logger.info("Deactivating logging facility")
            self.log_active = False

    def log_running(self):
        return self.logger is not None and self.log_active

    def create_tables(self):

        if self.log_running():
            self.logger.debug(f"Creating tables (from internal configuration)")
        tables = self.config["tables"]
        table_creation_commands = _config_tables_to_commands(tables)
        if self.log_running():
            self.logger.debug(f"----> Executing table creation commands <----")
            for cmd in table_creation_commands:
                self.logger.debug(f"\t- {cmd}")
        for cmd in table_creation_commands:
            _execute_command(self.cur, cmd)
        if self.log_running():
            self.logger.debug(
                f"Processed tables (type {type(tables)})\n{'*'*80}\n{tables}\n{'*'*80}"
            )
        self.retrieve_metadata()

    def get_tables(self):
        table_command = "SELECT name FROM sqlite_master WHERE type='table';"
        tables = _retrieve_data(self.cur, table_command)
        # for table in tables:
        if self.log_running():
            self.logger.debug([table_name[0] for table_name in tables])
        return [table_name[0] for table_name in tables]

    def retrieve_metadata(self):
        tables = self.get_tables()
        if self.log_running():
            self.logger.debug(f"Retrieved table data: {tables}")
        field_command = 'PRAGMA table_info("{}");'
        for t in tables:
            fields = _retrieve_data(self.cur, field_command.format(t))
            if self.log_running():
                self.logger.debug(f"Retrieved field info {fields}")
            if t in self.meta_info["tables"]:
                if self.log_running():
                    self.logger.debug(
                        f"Found already-defined table {t}. Skipping re-definition"
                    )
                continue
            self.meta_info["tables"][t] = self.table_to_tuples_for_entry(t, fields)
        if self.log_running():
            self.logger.debug(
                f"Retained table meta-information:\n{self.meta_info['tables']}"
            )

    def table_to_tuples_for_entry(self, table, fields):
        if self.log_running():
            self.logger.debug(
                f"Table conversion: Got table {table} and fields {fields}"
            )
            self.logger.debug(f"---> Table conversion is {string_to_camel_case(table)}")
            self.logger.debug(
                f"Field conversion is {[(field[1], SQLInterface.sqlite_types[field[2].upper()]) for field in fields if field[1] != 'id']}"
            )
        # NB: This ignores the 'id' field on the assumption that it is an AUTOINCREMENTing field handled by the database
        return NamedTuple(
            string_to_camel_case(table),
            [
                # *Pylance isn't perfect, and gets confused with this comprehension. Hence, `type: ignore`*
                (field[1], SQLInterface.sqlite_types[field[2].upper()])  # type: ignore
                for field in fields
                if field[1] != "id"
            ],
        )

    def get_data_entry_interfaces(self):
        return copy.deepcopy(self.meta_info["tables"])

    def insert_rows(self, data: dict):
        for table, data_list in data.items():
            assert table in self.meta_info["tables"], (
                f"Database interface doesn't have table {table}, aborting"
                f"\nAvailable rows are {self.meta_info['tables'].keys()}"
            )
            field_names = self.meta_info["tables"][table].__annotations__.keys()
            values = []

            if self.log_running():
                self.logger.debug(f"Found field name information: {field_names}")
                self.logger.debug("Received data:")
                for item in data_list:
                    self.logger.debug(f"\t- {item}")
            for item in data_list:
                values.append(str(tuple(item)))
            insert_command = 'INSERT INTO "{}" ({}) VALUES {};'.format(
                table,
                ",".join(field_names),
                ",".join(values),
            )
            if self.log_running():
                self.logger.debug(f"Issuing insertion command")
                self.logger.debug(insert_command)
            try:
                _execute_command(self.cur, insert_command)
            except Exception as e:
                raise e
            self.conn.commit()

    def delete_rows(self, data: dict):
        """
        data: 'table': [data_obj1, data_obj2, ...]
        Deletes data (from NamedTuple objects for each table)
            where fields match those found in data_obj
            e.g. obj: Apple has fields 'alice', 'bob'
            deletion for all rows where bob == XYZ requires format
            {'table name': [Apple(None, 'XYZ')]}
            None-field indicates non-inclusion in deletion criterion
        """
        for table, list_to_delete in data.items():
            deletion_cmd = 'DELETE FROM "{}" WHERE ({}) == {};'
            for item in list_to_delete:
                fields = item._fields
                field_list = [
                    f'"{f}"' for idx, f in enumerate(fields) if item[idx] is not None
                ]
                if self.log_running():
                    self.logger.debug(f"Found a deletion field list {field_list}")
                    self.logger.debug(
                        "Running deletion command {}".format(
                            deletion_cmd.format(
                                table,
                                ",".join(field_list),
                                ",".join([f'"{i}"' for i in item if i is not None]),
                            ),
                        )
                    )
                _execute_command(
                    self.cur,
                    deletion_cmd.format(
                        table,
                        ",".join(field_list),
                        ",".join([f'"{i}"' for i in item if i is not None]),
                    ),
                )
                self.conn.commit()

    def retrieve_rows(self, query: str):
        """
        query: Must be a well-formed query for the SQLite database
        (I assume too much complexity is possible from applications, here
         so I leave simplicity/compexity of implementation to the application.
         This package only handles the cursor/connection itself)
        """
        if self.log_running():
            self.logger.debug(f"Received data query \n\t---> {query}")
        try:
            data = _retrieve_data(self.cur, query)
            return data
        except Exception as e:
            raise

    def remove_tables(self):
        raise NotImplementedError("TBD!")