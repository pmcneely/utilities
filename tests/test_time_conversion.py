import time
import unittest

from utilities import (
    TimeConverter,
)


class TestTimeConverter(unittest.TestCase):

    date = "2020-03-17 23:59:59"  # Y/M/D 24H/Min/Sec
    old_date = "1918-02-01 00:00:00"

    def test_time_conversion(self):
        print(f"{'*'*20}{'1. Testing time conversion':^40}{'*'*20}")
        converter = TimeConverter()
        new_epoch = converter.human_to_epoch(TestTimeConverter.date)
        old_epoch = converter.human_to_epoch(TestTimeConverter.old_date)
        self.assertEqual(new_epoch, 1584503999.0)
        self.assertEqual(old_epoch, -1638298800.0)
        self.assertEqual(TestTimeConverter.date, converter.epoch_to_human(new_epoch))
        self.assertEqual(TestTimeConverter.old_date, converter.epoch_to_human(old_epoch))