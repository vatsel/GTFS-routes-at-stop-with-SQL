import unittest
import os
import sqlite3

from sys                import argv
from GTFSProcessor      import ZIPImporter
from GTFSProcessor      import Routes

DB_FILENAME = 'db.sqlite'
USAGE_STR = '''Usage: 
            tests.py [optional-database-path]'''

class TestDatabase(unittest.TestCase):
    def setUp(self):
        if not os.path.exists(DB_FILENAME):
            raise FileNotFoundError('''Running tests requires a valid \
database file named %s in this directory''' % DB_FILENAME )
        self.db_connection = sqlite3.connect(DB_FILENAME)
        self.cursor = self.db_connection.cursor()

    def test_Stop_Trip_Table(self):
        cursor_obj = self.cursor.execute('SELECT * FROM Stop_Trip;')
        col_names = [desc[0] for desc in cursor_obj.description]
        expected = set(['stop_id', 'trip_id', 'departure_time_in_sec'])
        self.assertSetEqual(expected, set(col_names),
            msg= "Expected : %s\nResult : %s" 
                % (str(expected), str(col_names)))

    def test_Route_Table(self):
        cursor_obj = self.cursor.execute('SELECT * FROM Route;')
        col_names = [desc[0] for desc in cursor_obj.description]
        expected = set(['id', 'short_name', 'long_name'])
        self.assertSetEqual(expected, set(col_names),
            msg= "Expected : %s\nResult : %s" 
                % (str(expected), str(col_names)))

class RouteFromStopMethodsTest(unittest.TestCase):
    def setUp(self):
        if not os.path.exists(DB_FILENAME):
            raise FileNotFoundError('''Running tests requires a valid \
database file named %s in this directory''' % DB_FILENAME )
        self.db_connection = sqlite3.connect(DB_FILENAME)
        self.cursor = self.db_connection.cursor()

    def test_specific_stop_exists(self):
        self.assertTrue(Routes.check_if_stop_exists(DB_FILENAME, '5644'))

    def test_nonexistent_stop_returns_false(self):
        self.assertFalse(Routes.check_if_stop_exists(DB_FILENAME, '9999999123'))

    def test_get_route_name_from_stop_id_with_nonexistent_id(self):
        expected = ""
        result = Routes.get_stop_name(DB_FILENAME, '99999999999123335')
        self.assertEqual(expected, result)

    def test_get_route_name_from_stop_id_with_specific_values(self):
        expected = "ch. Carrefour de la Rive-Sud et ch. de Touraine"
        result = Routes.get_stop_name(DB_FILENAME, '5602')
        self.assertEqual(expected, result)

    def test_get_route_ids_from_stop_with_specific_values(self):
        expected = set([29,74])
        result = Routes.get_route_ids_passing_through_stop(DB_FILENAME, '5644')
        self.assertSetEqual(expected, result)

    def test_get_route_ids_from_stop_with_specific_values_2(self):
        expected = set([37])
        result = Routes.get_route_ids_passing_through_stop(DB_FILENAME, '2635')
        self.assertSetEqual(expected, result)

    def test_get_route_ids_from_stop_returns_ints(self):
        result = Routes.get_route_ids_passing_through_stop(DB_FILENAME, '5644')
        for item in result:
            self.assertIs(type(item), int, msg="item is %s" % type(item))

    def test_get_route_ids_from_stop_with_orphan_stop_returns_empty_set(self):
        expected = set()
        result = Routes.get_route_ids_passing_through_stop(DB_FILENAME, '9999')
        self.assertSetEqual(expected, result)

    def test_get_route_from_stop_id_with_nonexistent_id_returns_empty_set(self):
        expected = set()
        result = Routes.get_route_ids_passing_through_stop(DB_FILENAME,
                '181221')
        self.assertSetEqual(expected, result)

    def test_get_route_short_name_with_specific_value(self):
        expected = 'T18'
        result = Routes.get_route_short_name(DB_FILENAME, '818')
        self.assertEqual(expected, result,"actual value = %s" % result)

    def test_get_route_short_name_with_specific_value_2(self):
        expected = 'T97'
        result = Routes.get_route_short_name(DB_FILENAME, '897')
        self.assertEqual(expected, result,"actual value = %s" % result)

    def test_get_route_short_name_with_nonexistent_id_returns_empty_str(self):
        expected = ''
        result = Routes.get_route_short_name(DB_FILENAME,
                '120003123')
        self.assertEqual(expected, result,"actual value = %s" % result)

    def test_get_route_long_name_with_specific_value(self):
        expected = 'Collectivité nouvelle'
        result = Routes.get_route_long_name(DB_FILENAME, '29')
        self.assertEqual(expected, result,"actual value = %s" % result)

    def test_get_route_long_name_with_specific_value_2(self):
        expected = 'Secteur B / Mountainview'
        result = Routes.get_route_long_name(DB_FILENAME, '32')
        self.assertEqual(expected, result,"actual value = %s" % result)

    def test_get_route_long_name_with_specific_value_3(self):
        expected = 'Taxi - St-Bruno - St-Basile'
        result = Routes.get_route_long_name(DB_FILENAME, '894')
        self.assertEqual(expected, result,"actual value = %s" % result)

    def test_get_route_long_name_with_nonexistent_id_returns_empty_str(self):
        expected = ''
        result = Routes.get_route_long_name(DB_FILENAME, '120003123')
        self.assertEqual(expected, result,"actual value = %s" % result)

    def test_get_earliest_service_for_stop_on_trip_with_specific_val(self):
        expected = '05:40:36'
        result = Routes.get_earliest_service_for_stop_on_trip(DB_FILENAME,
                '29','5644')
        self.assertEqual(expected, result, msg="returned = %s" % result)

    def test_get_earliest_service_for_stop_on_trip_with_specific_val_2(self):
        expected = '05:31:00'
        result = Routes.get_earliest_service_for_stop_on_trip(DB_FILENAME,
                '74','5644')
        self.assertEqual(expected, result, msg="returned = %s" % result)

    def test_get_latest_service_for_stop_on_trip_with_specific_val(self):
        expected = '25:28:44'
        result = Routes.get_latest_service_for_stop_on_trip(DB_FILENAME,
                '29','5644')
        self.assertEqual(expected, result, msg="returned = %s" % result)

    def test_get_latest_service_for_stop_on_trip_with_specific_val_2(self):
        expected = '25:22:00'
        result = Routes.get_latest_service_for_stop_on_trip(DB_FILENAME,
                '74','5644')
        self.assertEqual(expected, result, msg="returned = %s" % result)

    def test_get_latest_service_for_nonexistent_stop_returns_empty_str(self):
        expected = ''
        result = Routes.get_latest_service_for_stop_on_trip(DB_FILENAME,
                '74','10000000000000000')
        self.assertEqual(expected, result, msg="returned = %s" % result)

    def test_get_earliest_service_for_nonexistent_stop_returns_empty_str(self):
        expected = ''
        result = Routes.get_earliest_service_for_stop_on_trip(DB_FILENAME,
                '74','10000000000000000')
        self.assertEqual(expected, result, msg="returned = %s" % result)

    def test_get_latest_service_for_nonexistent_route_returns_empty_str(self):
        expected = ''
        result = Routes.get_latest_service_for_stop_on_trip(DB_FILENAME,
                '10000000000000000000','5644')
        self.assertEqual(expected, result, msg="returned = %s" % result)

    def test_get_earliest_service_for_nonexistent_route_returns_empty_str(self):
        expected = ''
        result = Routes.get_earliest_service_for_stop_on_trip(DB_FILENAME,
                '1000000000000000000000','5644')
        self.assertEqual(expected, result, msg="returned = %s" % result)

    def test_seconds_to_str_high_val(self):
        expected = '34:01:45'
        result = Routes._seconds_to_str(122505)
        self.assertEqual(expected, result, msg="returned = %s" % result)

    def test_seconds_to_str_minimum_val(self):
        expected = '00:00:00'
        result = Routes._seconds_to_str(0)
        self.assertEqual(expected, result, msg="returned = %s" % result)

if __name__ == '__main__':
    if not os.path.exists(DB_FILENAME):
        print('Database %s is required to be in the same directory as tests.py:\n%s' % (DB_FILENAME, os.getcwd()))
        exit()

    unittest.main()
