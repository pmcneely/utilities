from dataclasses import fields
from posixpath import basename
from typing import Type
import unittest
import json
import numpy as np
from numpy.core.fromnumeric import mean, std
import numpy.random as npr

npr.seed(42)
import os
import string

from pathlib import Path

from utilities import (
    read_yaml,
    validate_config,
    remove_file,
    SQLInterface,
)

# Import appropriate exception from sqlite
from sqlite3 import IntegrityError


class TestSQLInterface(unittest.TestCase):

    test_path = Path(__file__).resolve().parent
    sample_config = read_yaml(
        os.path.abspath(os.path.join(test_path, "./sample_configs/test_config.yaml"))
    )
    dbi = None

    def test_1_validate_config(self):
        print(f"\n{'*'*20}{'1. Testing config validation':^40}{'*'*20}")
        config = self.__class__.sample_config
        self.assertIsNone(validate_config(config))

        # Hacky because unittest doesn't like doing
        # stateful inter-test (ie, integration/component) testing
        # ¯\_(ツ)_/¯
        try:
            db_path = os.path.abspath(
                os.path.join(
                    os.path.expanduser(config["project dir"]) + config["db file"]
                )
            )
            print(f"Removing any previous database file at\n\t{db_path}")
            remove_file(db_path)
        except Exception as e:
            print(f"Did not find previous test DB, continuing with testing!")
            pass

    def test_2_connect(self):

        print(f"\n{'*'*20}{'2. Testing database connection':^40}{'*'*20}")
        config = self.__class__.sample_config

        self.__class__.dbi = SQLInterface(config, log_name="test")

        # Turn off logging!
        self.__class__.dbi.deactivate_logging()

    def test_3_get_table_entry_information(self):
        self.assertIsNotNone(self.__class__.dbi)
        # XXX: self.__class__.dbi.activate_logging()
        print(f"\n{'*'*20}{'3. Testing table creation':^40}{'*'*20}")

        self.__class__.dbi.retrieve_metadata()
        # XXX: self.__class__.dbi.deactivate_logging()

    def test_4_get_tables(self):
        self.assertIsNotNone(self.__class__.dbi)
        print(f"\n{'*'*20}{'4. Testing table retrieval':^40}{'*'*20}")
        self.__class__.dbi.get_tables()

    def test_5_insert_only_required_fields(self):
        self.assertIsNotNone(self.__class__.dbi)
        print(
            f"\n{'*'*20}{'5. Testing data creation and minimal insertion':^40}{'*'*20}"
        )

        # XXX: self.__class__.dbi.activate_logging()
        # Required fields / update fields are returned as ([keys], [required fields])
        apple_fields = self.__class__.dbi.get_data_entry_fields("apple pie")
        banana_fields = self.__class__.dbi.get_data_entry_fields("bananas foster")

        # API is a little clunky for now
        apple_requirements = [
            field for field_list in apple_fields for field in field_list
        ]
        banana_requirements = [
            field for field_list in banana_fields for field in field_list
        ]

        self.assertEqual(apple_requirements, ["bonnie"])
        self.assertEqual(banana_requirements, ["erin", "claire"])

        self.__class__.dbi.insert_rows("apple pie", apple_requirements, [(0,)])
        self.__class__.dbi.insert_rows("bananas foster", banana_requirements, [(0, 0)])

        # XXX: self.__class__.dbi.deactivate_logging()

    def test_6_insert_full_row_data(self):

        self.assertIsNotNone(self.__class__.dbi)
        print(f"\n{'*'*20}{'6. Testing full row data insertion':^40}{'*'*20}")

        # Get all fields for two tables
        apple_fields = self.__class__.dbi.get_all_table_fields("apple pie")
        banana_fields = self.__class__.dbi.get_all_table_fields("bananas foster")

        # Data is expected to be sent in python objects as [fields], [(table_vals1), (table_vals2), ...]}
        new_apple_data = [(1, 2, 5), ("metal gear", "animal crossing", "TEXT KEY!")]
        new_banana_data = [
            ("new data", 42.4, "ABCD"),
            ("new data", "different key!", "BCDE"),
        ]

        # XXX: self.__class__.dbi.activate_logging()
        # TODO: Move small exemplar(s) to documentation, leave large data test in place
        # Add a little data

        self.__class__.dbi.insert_rows("apple pie", apple_fields, new_apple_data)
        self.__class__.dbi.insert_rows("bananas foster", banana_fields, new_banana_data)

        # NB - Data with an auto-incrementing PRIMARY KEY (`apple pie` in testing)
        #      can have duplicate entries
        #      Data in tables defined `WITHOUT ROWID` should report conflict with previous keys
        self.__class__.dbi.insert_rows("apple pie", apple_fields, new_apple_data)
        self.assertRaises(
            IntegrityError,
            self.__class__.dbi.insert_rows,
            "bananas foster",
            banana_fields,
            new_banana_data,
        )
        # XXX: self.__class__.dbi.deactivate_logging()

        # Add lots of data
        # e.g. "AAA":"DDD"
        letter_ids = [
            i + j + k
            for i in string.ascii_uppercase[:4]
            for j in string.ascii_uppercase[:4]
            for k in string.ascii_uppercase[:4]
        ]
        values = npr.uniform(low=0, high=1.0, size=(len(letter_ids), 2))
        new_bananas = [
            (values[idx, 0], values[idx, 1], i) for idx, i in enumerate(letter_ids)
        ]
        self.__class__.dbi.insert_rows("bananas foster", banana_fields, new_bananas)

        # Add _lots_ more data in the 'Foreign key' test table (ie, `banana derivative`)
        detail_fields = self.__class__.dbi.get_all_table_fields("banana details")
        initial_values = npr.uniform(low=50, high=200, size=(len(letter_ids), 2))
        banana_details = []
        for index, banana_id in enumerate(letter_ids):
            random_walk_values = npr.normal(loc=0, scale=5, size=(50, 2))
            detail_values = np.concatenate(
                (initial_values[index, np.newaxis], random_walk_values)
            ).cumsum(0)
            detail_values[:, 1] = np.round(detail_values[:, 1])
            banana_details.extend(
                (banana_id, counter, detail_values[idx, 0], detail_values[idx, 1])
                for counter, idx in enumerate(range(len(detail_values)))
            )
        self.__class__.dbi.insert_rows("banana details", detail_fields, banana_details)

    def test_7_delete_data(self):
        self.assertIsNotNone(self.__class__.dbi)
        print(f"\n{'*'*20}{'7. Testing data deletion':^40}{'*'*20}")

        # XXX: Remove the activate/deactivate logging jazz from tests. Document elsewhere
        self.__class__.dbi.activate_logging()  # XXX:
        apple_fields = self.__class__.dbi.get_all_table_fields("apple pie")
        # List the fields to determine order as needed
        # TODO: Document field order elsewhere
        print(f"The order of fields in 'apple pie' is {apple_fields}")
        # Test removal by non-primary key
        removal_fields = [
            apple_fields[2]
        ]  # 'bonnie'; need to know fields for deletion ahead of time
        bad_apple = [("animal crossing",)]
        self.__class__.dbi.delete_rows("apple pie", removal_fields, bad_apple)

        # Test removal by primary key
        banana_fields = self.__class__.dbi.get_all_table_fields("bananas foster")
        # TODO: Document field order elsewhere
        removal_fields = [banana_fields[2]]  # 'erin'
        # Test removal by primary key
        rotten_banana = [("BCDE",)]
        self.__class__.dbi.delete_rows("bananas foster", removal_fields, rotten_banana)

        # TODO: Remove the activate/deactivate logging jazz from tests. Document elsewhere
        self.__class__.dbi.deactivate_logging()  # XXX:

    def test_8_retrieve_data(self):

        self.assertIsNotNone(self.__class__.dbi)
        print(f"\n{'*'*20}{'8. Testing data retrieval (1 table)':^40}{'*'*20}")
        # Require the application to format queries, as queries are better formed at the source
        # XXX: self.__class__.dbi.activate_logging()
        query_strings = [
            f'"erin" = "{q}" OR '
            for q in [
                "B" + i + j
                for i in string.ascii_uppercase[:4]
                for j in string.ascii_uppercase[:4]
            ]
        ]
        query = 'SELECT * FROM "bananas foster" WHERE ({});'.format(
            " ".join(query_strings).rstrip(" OR ")
        )
        results = self.__class__.dbi.retrieve_rows(query)
        self.assertEqual(len(results), 16)
        # XXX: self.__class__.dbi.deactivate_logging()

    def test_9_partial_data_update(self):
        self.assertIsNotNone(self.__class__.dbi)
        print(f"\n{'*'*20}{'9. Testing data update (1 row)':^40}{'*'*20}")
        self.__class__.dbi.activate_logging()

        query = 'SELECT * FROM "bananas foster" WHERE ("erin") = "AAB";'
        results = list(
            self.__class__.dbi.retrieve_rows(query)[0]
        )  # Only one result on primary key
        new_value = 1.0  # Value for update!
        results[1] = 1.0  # Updated result!
        all_bananas_fields = self.__class__.dbi.get_all_table_fields("bananas foster")
        new_banana = [tuple(results)]

        # Can't use insert!
        self.assertRaises(
            IntegrityError,
            self.__class__.dbi.insert_rows,
            "bananas foster",
            all_bananas_fields,
            new_banana,
        )

        # ...need to use an update method
        # Get the keys and fields for update
        fields_for_update = self.__class__.dbi.get_data_entry_fields(
            "bananas foster", ["dave"]
        )
        # Clunky API currently :/
        fields_for_search = fields_for_update[0]
        fields_for_update = fields_for_update[1]

        # Update is a list of paired-tuples:
        #   - First value is key values (must be same length as search fields)
        #   - Second value is updated field values (must be same length as fields to update)
        banana_update = [(("AAB",), (new_value,))]
        update_info = {
            "keys": fields_for_search,
            "update fields": fields_for_update,
            "update values": banana_update,
        }

        self.__class__.dbi.update_rows("bananas foster", update_info)

    def test_10_retrive_data_on_join(self):
        # TODO...?
        pass

    def test_11_remove_tables(self):
        # TODO...?
        pass

    def test_12_delete_database(self):
        # TODO...?
        pass


if __name__ == "__main__":
    unittest.main()
