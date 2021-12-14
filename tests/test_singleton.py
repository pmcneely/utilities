import unittest

from utilities import (
    Borg,
)

class TestBorg(unittest.TestCase):

    def test_borg(self):

        print(f"{'*'*20}{'1. Testing Borg functionality':^40}{'*'*20}")
        six_of_nine = Borg()
        for i in range(3):
            self.assertEqual(six_of_nine.next_value(), i)
        seven_of_nine = Borg()
        self.assertEqual(seven_of_nine.next_value(), 3)


if __name__ == "__main__":

    unittest.main()