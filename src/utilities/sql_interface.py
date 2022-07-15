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
import string

import pandas as pd

from typing import NamedTuple

from .log_utilities import (
    create_logger,
    register_logger,
    ControlledLogger,
)
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


def validate_config(config: dict):
    try:
        assert {"project dir", "db config", "db file"}.issubset(
            config.keys()
        ), "Invalid configuration for SQLLite interface"
        project_dir = os.path.abspath(os.path.expanduser(config["project dir"]))
        db_config_file = config["db config"]
        config_path = os.path.join(project_dir, db_config_file)
        assert os.path.isfile(
            config_path
        ), f"No database defnition file found at {config_path}"
    except AssertionError:
        raise ValueError("Invalid configuration. See stack trace for information")


class SQLInterface:
    def __init__(self, config: dict = None, log_name: str = None):
        """
        'config' should include the following information:
            'project dir': Base directory for where data should be stored
                For SQLite databases, this will be the root folder
                which contains the database and the 'DB' folder
                that contains linked information.
            'db config': SQL script defining the database
            'db file': Database file (SQLite-only!) at `path`, optionally already created
        """

        assert (
            config is not None
        ), "A configuration for the database must be supplied, Aborting"

        if log_name is not None:
            # Check if the logger has been created
            if log_name in logging.root.manager.loggerDict:
                self.logger = logging.getLogger(log_name).getChild("DBI")
            else:
                register_logger(ControlledLogger)
                create_logger(log_name=log_name)
                self.logger = logging.getLogger(log_name)
                self.logger.info(
                    "Creating a logger with default configuration."
                    " The preferred use-case is to create the logger"
                    " prior to initializing this interface"
                )
        else:
            self.logger = register_logger(ControlledLogger)
            log_name = "SQLite Database Interface - Default logger"
            create_logger(log_name=log_name)
            self.logger = logging.getLogger(log_name)
            self.logger.warning(
                f"Creating a DB interface logger with default settings (name {log_name}). "
                " Use <DBInterface>.deactivate_logging() to silence."
                " See documentation for further information"
            )
        self.logger.debug("Validating configuration")
        validate_config(config)
        self.config = config

        db_name = config["db file"]
        base_path = os.path.abspath(os.path.expanduser(config["project dir"]))
        self.db_path = base_path
        self.db = os.path.join(base_path, db_name)
        self.logger.debug(f"Attempting to connect to database at path {self.db}")
        db_exists = True
        if not os.path.isfile(self.db):
            self.logger.info(f" --- Did not find a database file at {self.db}")
            self.logger.info(" --- Attempting to create the database")
            db_exists = False
        self.conn = sqlite3.connect(self.db)
        self.cur = self.conn.cursor()
        if not db_exists:
            try:
                config_path = os.path.join(base_path, config["db config"])
                with open(config_path, "r") as db_config:
                    sql_string = db_config.read()
                    self.cur.executescript(sql_string)
                db_config.close()
            except:
                raise
        self.meta_info = {"tables": {}}
        self.retrieve_metadata()

    def activate_logging(self):
        if isinstance(self.logger, ControlledLogger):
            self.logger.activate()
            self.logger.info("Activating logging facility")

    def deactivate_logging(self):
        if isinstance(self.logger, ControlledLogger):
            self.logger.deactivate()
            self.logger.info("Deactivated logging facility")

    def get_tables(self):
        table_command = "SELECT name FROM sqlite_master WHERE type='table';"
        tables = _retrieve_data(self.cur, table_command)
        self.logger.debug([table_name[0] for table_name in tables])
        return [table_name[0] for table_name in tables]

    def retrieve_metadata(self):
        tables = self.get_tables()
        self.logger.debug(f"Retrieved table data: {tables}")
        field_command = 'PRAGMA table_info("{}");'
        for table in tables:
            fields = _retrieve_data(self.cur, field_command.format(table))
            self.logger.debug(f"Retrieved field info {fields}")
            if table in self.meta_info["tables"]:
                self.logger.debug(
                    f"Found already-defined table {table}. Skipping re-definition"
                )
                continue
            self.meta_info["tables"][table] = self.get_table_information(table, fields)
        self.logger.debug(
            f"Retained table meta-information:\n{self.meta_info['tables']}"
        )

    def get_table_information(self, table, fields):
        self.logger.debug(f"Table conversion: Got table {table} and fields {fields}")
        keys, required = [], []
        field_names = {}
        for f in fields:
            field_names[f[1]] = (
                f[0] + 1,
                f[2].upper(),
            )  # Tuple(Field index, Field type)
            if f[5] > 0:
                # It's a key! The keys list preserves key index order
                keys.append(f[1])
            if f[3] > 0:
                required.append(f[1])
        return {"fields": field_names, "keys": keys, "required": required}

    def get_all_table_fields(self, table):
        assert (
            table in self.meta_info["tables"]
        ), f"Did not find requested table {table}. Aborting"
        return list(self.meta_info["tables"][table]["fields"].keys())

    def get_data_entry_fields(self, table: str, fields: list = None):

        # Returns (keys, [required_fields|fields_for_update])

        table_info = self.meta_info["tables"][table]
        db_fields = table_info["fields"]
        db_keys = table_info["keys"]
        db_required_fields = table_info["required"]
        if fields is None:
            return (db_keys, db_required_fields)
        else:
            assert isinstance(
                fields, list
            ), "Fields argument must be a list or None [default: return keys and required fields only]"
            assert set(fields).issubset(db_fields.keys()), (
                "Fields passed are not a subset of table fields\n"
                f"--- Requested: {fields}\n"
                f"--- Fields available: {db_fields.keys()}"
            )
            return (db_keys, fields)

    def insert_rows(self, table: str, fields: list, data: list):

        assert isinstance(
            data, list
        ), "Data must be provided as a list (even singleton entries!)"

        self.logger.debug(f"Found fields for insertion: {fields}")
        self.logger.debug(f"Received data:\n{data}")
        fields = [f'"{field}"' for field in fields]
        insert_command = 'INSERT OR REPLACE INTO "{}" ({})'.format(
            table,
            ", ".join(fields),
        )
        insert_command += " VALUES {};".format(
            ",".join(f"{tup}" if len(tup) > 1 else f"({tup[0]})" for tup in data)
        )
        self.logger.debug(f"Issuing insertion command")
        self.logger.debug(insert_command)
        try:
            _execute_command(self.cur, insert_command)
        except Exception as e:
            print(f" --- Exception {e} ---\nLast command:\n\t{insert_command}")
            raise
        self.conn.commit()

    def update_rows(self, table: str, update_info: dict):

        insert_command = f'UPDATE "{table}" '
        key_fields = update_info["keys"]
        update_fields = update_info["update fields"]
        new_values = update_info["update values"]
        for nv in new_values:
            assert len(key_fields) == len(
                nv[0]
            ), "Mismatch in key search parameters, aborting"
            assert len(update_fields) == len(
                nv[1]
            ), "Mismatch in update field parameters, aborting"
            set_values = " AND ".join(
                ['"{}" = "{}"'.format(a, b) for a, b in zip(update_fields, nv[1])]
            ).rstrip(" AND ")
            where_values = " AND ".join(
                ['"{}" = "{}"'.format(a, b) for a, b in zip(key_fields, nv[0])]
            ).rstrip(" AND ")
            cmd = insert_command + "SET {} WHERE {};".format(
                set_values,
                where_values,
            )
            self.logger.debug(f"Issuing insertion command\n\t ---> {cmd}")
            try:
                _execute_command(self.cur, cmd)
            except Exception as e:
                raise e
        self.conn.commit()

    def delete_rows(self, table: str, fields: list, data: list):
        deletion_cmd = f'DELETE FROM "{table}" WHERE '  #  ({}) == {};'
        self.logger.debug(f"Found a deletion field list {fields}")
        for item in data:
            assert len(item) == len(
                fields
            ), f"Mismatch in deletion fields {fields} with query {item}. Aborting."
            cmd = deletion_cmd
            for idx, i in enumerate(item):
                cmd += '("{}") == "{}" AND '.format(fields[idx], i)
            cmd = cmd.rstrip(" AND ")
            cmd += ";"
            self.logger.debug("Running deletion command {}".format(cmd))
            try:
                _execute_command(self.cur, cmd)
            except Exception as e:
                raise
            self.conn.commit()

    def retrieve_rows(self, query: str):
        """
        query: Must be a well-formed query for the SQLite database
        (I assume too much complexity is possible from applications, here
         so I leave simplicity/compexity of implementation to the application.
         This package only handles the cursor/connection itself)
        """
        self.logger.debug(f"Received data query \n\t---> {query}")
        try:
            return _retrieve_data(self.cur, query)
        except Exception as e:
            raise

    def remove_tables(self):
        raise NotImplementedError("TBD!")

    def execute_pandas_query(self, command: str):
        # Execute a pandas-formatted query with table name known (fields optional)
        # ...What could go wrong?
        try:
            return pd.read_sql_query(command, self.conn)
        except Exception as e:
            raise
