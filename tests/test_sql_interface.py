from posixpath import basename
import unittest
import json
import numpy as np
from numpy.core.fromnumeric import mean, std
import numpy.random as npr

npr.seed(42)
import os
import string

from collections import namedtuple
from pathlib import Path

from utilities import (
    read_yaml,
    validate_config,
    remove_file,
    SQLInterface,
)


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

    def test_5_create_data(self):
        self.assertIsNotNone(self.__class__.dbi)
        print(f"\n{'*'*20}{'5. Testing data creation':^40}{'*'*20}")

        table_entry_objects = self.__class__.dbi.get_data_entry_interfaces()

        AppleEntry = table_entry_objects["apple pie"]
        BananaEntry = table_entry_objects["bananas foster"]

        test_apple = AppleEntry(1, 2)
        test_banana = BananaEntry("python and sqlite", "are not type safe", "really!")
        self.assertEqual((1, 2), tuple(test_apple))
        self.assertEqual(
            ("python and sqlite", "are not type safe", "really!"), tuple(test_banana)
        )

    def test_6_insert_data(self):

        self.assertIsNotNone(self.__class__.dbi)
        print(f"\n{'*'*20}{'6. Testing data insertion':^40}{'*'*20}")
        # Import appropriate exception from sqlite
        from sqlite3 import IntegrityError

        # Get objects appropriate for each
        table_entry_objects = self.__class__.dbi.get_data_entry_interfaces()
        AppleEntry = table_entry_objects["apple pie"]
        BananaEntry = table_entry_objects["bananas foster"]
        # Data is expected to be sent in python objects as {table: [table_obj1, table_obj2, ...]}
        apple_data = {
            "apple pie": [AppleEntry(1, 2), AppleEntry("metal gear", "animal crossing")]
        }
        banana_data = {
            "bananas foster": [
                BananaEntry("new data", 42.4, "ABCD"),
                BananaEntry("new data", "different key!", "BCDE"),
            ]
        }

        # XXX: self.__class__.dbi.activate_logging()
        # TODO: Move small exemplar(s) to documentation, leave large data test in place
        # Add a little data
        self.__class__.dbi.insert_rows(apple_data)
        self.__class__.dbi.insert_rows(banana_data)

        # NB - Data with an auto-incrementing PRIMARY KEY (`apple pie` in testing)
        #      can have duplicate entries
        #      Data in tables defined `WITHOUT ROWID` should report conflict with previous keys
        self.__class__.dbi.insert_rows(apple_data)
        self.assertRaises(IntegrityError, self.__class__.dbi.insert_rows, banana_data)
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
        table_entry_objects = self.__class__.dbi.get_data_entry_interfaces()
        BananaEntry = table_entry_objects["bananas foster"]
        new_bananas = {
            "bananas foster": [
                BananaEntry(values[idx, 0], values[idx, 1], i)
                for idx, i in enumerate(letter_ids)
            ]
        }
        self.__class__.dbi.insert_rows(new_bananas)

        # Add _lots_ more data in the 'Foreign key' test table (ie, `banana derivative`)
        BananaDetail = table_entry_objects["banana details"]
        initial_values = npr.uniform(low=50, high=200, size=(len(letter_ids), 2))
        banana_details = {"banana details": []}
        for index, banana_id in enumerate(letter_ids):
            random_walk_values = npr.normal(loc=0, scale=5, size=(50, 2))
            detail_values = np.concatenate(
                (initial_values[index, np.newaxis], random_walk_values)
            ).cumsum(0)
            detail_values[:, 1] = np.round(detail_values[:, 1])
            banana_details["banana details"].extend(
                BananaDetail(banana_id, detail_values[idx, 0], detail_values[idx, 1])
                for idx in range(len(detail_values))
            )
        self.__class__.dbi.insert_rows(banana_details)

    def test_7_delete_data(self):
        self.assertIsNotNone(self.__class__.dbi)
        print(f"\n{'*'*20}{'7. Testing data deletion':^40}{'*'*20}")

        # XXX: Remove the activate/deactivate logging jazz from tests. Document elsewhere
        # XXX: self.__class__.dbi.activate_logging()
        teo = self.__class__.dbi.get_data_entry_interfaces()
        AppleEntry = teo["apple pie"]
        apple_fields = AppleEntry._fields
        # List the fields to determine order as needed
        # TODO: Document field order elsewhere
        print(f"The order of fields in {AppleEntry} is {apple_fields}")
        # Test removal by non-primary key
        bad_apple = {"apple pie": [AppleEntry(None, "animal crossing")]}
        self.__class__.dbi.delete_rows(bad_apple)

        # Test removal by primary key
        BananaEntry = teo["bananas foster"]
        # TODO: Document field order elsewhere
        banana_fields = BananaEntry._fields
        # Test removal by primary key
        rotten_banana = {"bananas foster": [BananaEntry(None, None, "BCDE")]}
        self.__class__.dbi.delete_rows(rotten_banana)

        # TODO: Remove the activate/deactivate logging jazz from tests. Document elsewhere
        # XXX: self.__class__.dbi.deactivate_logging()

    def test_8_retrieve_data(self):

        self.assertIsNotNone(self.__class__.dbi)
        print(f"\n{'*'*20}{'8. Testing data retrieval (1 table)':^40}{'*'*20}")
        # Get the "B"-class dummy data
        to_retrieve = [
            "B" + i + j
            for i in string.ascii_uppercase[:4]
            for j in string.ascii_uppercase[:4]
        ]
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

    def test_9_retrive_data_on_join(self):
        # TODO...?
        pass

    def test_10_remove_tables(self):
        # TODO...?
        pass

    def test_11_delete_database(self):
        # TODO...?
        pass


if __name__ == "__main__":
    unittest.main()
