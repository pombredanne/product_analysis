import unittest
import datetime
from simulator import weeks_generator

class TestSimulator(unittest.TestCase):

    def test_week_generator(self):

        self.assertEqual(
            weeks_generator("2017/1/1", "2017/7/4")[0], datetime.datetime(2016, 12, 26)
        )


if __name__ == "__main__":

    unittest.main()


