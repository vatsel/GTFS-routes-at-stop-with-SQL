import os
import zipfile
import sqlite3

from shutil                     import rmtree
from operator                   import add
from functools                  import reduce


TEMP_DIR = 'temp/'
REQUIRED_GFTS_FILENAMES_SET = ['routes.txt','stops.txt', 'stop_times.txt',
        'trips.txt']

def _verify_zip_contains_required_GTFS_filenames(zip_path):
    zip_file = zipfile.ZipFile(zip_path)
    filenames_set = set(zip_file.namelist())
    for filename in REQUIRED_GFTS_FILENAMES_SET:
        if filename not in filenames_set:
            raise zipfile.BadZipfile('Archive is missing filename %s' 
                    % filename)


def _extract_to_temp_dir(zip_path):
    if os.path.exists(TEMP_DIR):
        rmtree(TEMP_DIR)
    zip_file = zipfile.ZipFile(zip_path)
    for filename in REQUIRED_GFTS_FILENAMES_SET:
        zip_file.extract(filename, path=TEMP_DIR)


def _create_sqlite_db(path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    cursor = connection.cursor()    
    cursor.execute('''CREATE TABLE Route 
                    (id INT PRIMARY KEY,
                    short_name VARCHAR(10),
                    long_name TEXT(150));''')
    cursor.execute('''CREATE TABLE Trip 
                    (id VARCHAR(100) PRIMARY KEY,
                    route_id INT,
                    FOREIGN KEY(route_id) REFERENCES Route(id));''')
    cursor.execute('''CREATE TABLE Stop 
                    (name TEXT(150),
                    id INT PRIMARY KEY);''')
    cursor.execute('''CREATE TABLE Stop_Trip
                    (stop_id INT,
                    trip_id VARCHAR(100),
                    departure_time_in_sec INT,
                    FOREIGN KEY(trip_id) REFERENCES Trip(id),
                    FOREIGN KEY(stop_id) REFERENCES Stop(id)
                    ); ''')
    cursor.execute('''CREATE TABLE Stop_Route
                    (route_id INT,
                    stop_id INT,
                    FOREIGN KEY(route_id) REFERENCES Route(id),
                    FOREIGN KEY(stop_id) REFERENCES Stop(id));''')
    connection.commit()
    return connection


def _get_file_column_to_index_map(filepath) -> dict:
    '''
    returns dict: {column_name : column_index , ... }
    '''
    f = open(filepath)
    line = f.readline()
    if '\n' not in line:
        raise Exception("%s is empty !" % filepath)
    columns = line.rstrip('\n').split(',')
    col_to_index_map = { col:i for i,col in enumerate(columns) }
    f.close()
    return col_to_index_map


def _insert_data_into_table(db_connection, columns_to_values_map, table_name):
    cursor = db_connection.cursor()
    column_names, values = zip(*columns_to_values_map.items())
    sql_query = 'INSERT INTO %s\n' % table_name \
        + '(' + ', '.join(['"%s"' % n for n in column_names]) \
        + ')\nVALUES (' \
        + ', '.join(['"%s"' % v for v in values]) + ');'
    cursor.execute(sql_query)


def _convert_time_data_to_seconds(time_string) -> int:
    '''expecting time_string formatted as HH:MM:SS'''
    number_list = [int(x) for x in time_string.split(':')]
    return reduce(add, [60 ** (2-i) * num for i, num in enumerate(number_list)])

        
def _populate_departure_time_in_sec(db_connection):
    f = open(TEMP_DIR+'stop_times.txt')
    col_to_index_map = _get_file_column_to_index_map(TEMP_DIR \
            + 'stop_times.txt')
    cursor = db_connection.cursor()
    for line in f.readlines()[1:]:
        line_items = line.rstrip('\n').split(',')
        time_string = line_items[ col_to_index_map['departure_time']]
        seconds = _convert_time_data_to_seconds(time_string)
        stop_id = line_items[ col_to_index_map['stop_id']]
        cursor.execute('''
                UPDATE Stop_Trip
                SET departure_time_in_sec = :seconds
                WHERE stop_id = :stop_id
                ''', {'seconds': seconds, 'stop_id': stop_id})
    cursor.commit()
    f.close()


def _insert_data_from_file_into_table(db_connection, filepath, db_table_name, 
        file_to_table_col_map):
    '''file_to_table_col_map structure:
    { file_col_name : table_col_name , ...  }
    '''
    col_to_index_map = _get_file_column_to_index_map(filepath)
    file_column_names, table_column_names = zip(*file_to_table_col_map.items())
    table_column_names = ['"%s"' % x for x in table_column_names]

    f = open(filepath)
    cursor = db_connection.cursor()
    for line in f.readlines()[1:]:
        line_items = line.rstrip('\n').split(',')

        sql_query = 'INSERT INTO %s\n(' % db_table_name
        sql_query +=  ', '.join(table_column_names) + ')\n VALUES ( '
        
        inserted_values = list()
        for file_col_name in file_column_names:
            file_column_index = col_to_index_map[file_col_name]
            inserted_values.append( line_items[file_column_index] )
        inserted_values = ['"%s"' % x for x in inserted_values]
        sql_query += ', '.join(inserted_values) + ' );'
        
        cursor.execute(sql_query)
    f.close()
    db_connection.commit()


def _process_routes_file(db_connection):
    mapping = {'route_id': 'id','route_short_name':'short_name',
            'route_long_name':'long_name'}
    _insert_data_from_file_into_table(db_connection, 
            TEMP_DIR + 'routes.txt', 'Route', mapping)


def _process_trips_file(db_connection):
    mapping = {'trip_id': 'id', 'route_id':'route_id'}
    _insert_data_from_file_into_table(db_connection, 
            TEMP_DIR + 'trips.txt', 'Trip', mapping)


def _process_stops_file(db_connection):
    mapping = {'stop_id':'id', 'stop_name':'name'}
    _insert_data_from_file_into_table(db_connection, 
            TEMP_DIR + 'stops.txt', 'Stop', mapping)


def _process_stop_times_file(db_connection):
    f = open(TEMP_DIR + 'stop_times.txt')
    col_to_index_map = _get_file_column_to_index_map(TEMP_DIR \
            + 'stop_times.txt')
    for line in f.readlines()[1:]:
        line_items = line.rstrip('\n').split(',')
        time_string = line_items[ col_to_index_map['departure_time']]
        seconds = _convert_time_data_to_seconds(time_string)
        stop_id = line_items[ col_to_index_map['stop_id']]
        trip_id  = line_items[ col_to_index_map['trip_id']]
        mapping = {
                'stop_id' : stop_id,
                'trip_id' : trip_id,
                'departure_time_in_sec' : seconds
                }
        _insert_data_into_table(db_connection, mapping, 'Stop_Trip')
    f.close()
    db_connection.commit()


def import_into_database(archive_path, sqlite_database_path):
    _verify_zip_contains_required_GTFS_filenames(archive_path)
    _extract_to_temp_dir(archive_path)
    if os.path.exists(sqlite_database_path):
        os.remove(sqlite_database_path)
    db_connection = _create_sqlite_db(sqlite_database_path)
    
    _process_routes_file(db_connection)
    print('Processed routes.')
    _process_trips_file(db_connection)
    print('Processed trips.')
    _process_stops_file(db_connection)
    print('Processed stops.')
    _process_stop_times_file(db_connection)
    print('Processed stop_times.')
    cursor = db_connection.cursor()
    route_stop_data = cursor.execute('''
                SELECT DISTINCT route_id,stop_id
                FROM Trip, Stop_Trip
                WHERE Trip.id = Stop_Trip.trip_id;
                ''').fetchall()
    for data_row in route_stop_data:
        col_to_value_map = {'route_id': data_row[0], 'stop_id': data_row[1]}
        _insert_data_into_table(db_connection, col_to_value_map, 'Stop_Route')
    db_connection.commit()
    print('Done.')
    if os.path.exists(TEMP_DIR):
        rmtree(TEMP_DIR) 
