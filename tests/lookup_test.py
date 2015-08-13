
import unittest
from utils.tiles_lookup import *

class TilesLookup(unittest.TestCase):
    """Internet connection is required"""

    def test_parse_date(self):
        self.assertEqual(datetime.date(1994, 06, 13), get_date('1994.06.13'))
        self.assertIsNone(get_date('1994.13.06'))
        self.assertIsNone(get_date('1994-06-13'))

    def test_directories_urls(self):
        tiles_base_dir = 'http://e4ftl01.cr.usgs.gov/MOLT/MOD14A1.005/'

        def date_test(date):
            return date.year == 2000 and 6 <= date.month <= 8

        self.assertEqual(len(get_tiles_directories_urls(tiles_base_dir, date_test)), 12)

    def test_tiles_files_urls(self):
        tiles_directory_url = 'http://e4ftl01.cr.usgs.gov/MOLT/MOD14A1.005/2000.06.09/'
        self.assertEqual(len(get_tiles_files_urls(tiles_directory_url)), 286)
