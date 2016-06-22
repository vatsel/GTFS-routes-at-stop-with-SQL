import sqlite3


def _seconds_to_str(seconds) -> str:
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return '%02d:%02d:%02d' % (h, m, s)


def check_if_stop_exists(db_filepath, stop_id) -> bool:
    db_connection = sqlite3.connect(db_filepath)
    cursor = db_connection.cursor()
    cursor.execute('''select id
                    from stop
                    where stop.id =:id;''', {"id" : stop_id})
    return len(cursor.fetchall()) > 0


def get_stop_name(db_filepath, stop_id) -> str:
    '''if there is no match, returns an empty str'''
    db_connection = sqlite3.connect(db_filepath)
    cursor = db_connection.cursor()
    cursor.execute('''select name
                    from stop
                    where stop.id =:id;''', {"id" : stop_id})
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        return ''
    
def get_route_ids_passing_through_stop(db_filepath, stop_id) -> set:
    '''if there is no match, returns an empty set'''
    db_connection = sqlite3.connect(db_filepath)
    cursor = db_connection.cursor()
    cursor.execute('''select route_id 
                    from stop_route
                    where stop_route.stop_id =:id;''', {"id" : stop_id})
    return set([tuple_[0] for tuple_ in cursor.fetchall()])


def get_route_short_name(db_filepath, route_id) -> str:
    '''If there is no match, returns an empty str'''
    db_connection = sqlite3.connect(db_filepath)
    cursor = db_connection.cursor()
    cursor.execute('''SELECT short_name
                    FROM Route
                    WHERE Route.id =:id;''', {'id' : route_id})
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        return ''


def get_route_long_name(db_filepath, route_id) -> str:
    '''If there is no match, returns an empty str'''
    db_connection = sqlite3.connect(db_filepath)
    cursor = db_connection.cursor()
    cursor.execute('''SELECT long_name
                    FROM Route
                    WHERE Route.id =:id;''', {'id' : route_id})
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        return ''


def get_latest_service_for_stop_on_trip(db_filepath, route_id, 
        stop_id) -> str:
    '''If there is no match, returns an empty str'''
    db_connection = sqlite3.connect(db_filepath)
    cursor = db_connection.cursor()
    cursor.execute('''SELECT max(Stop_Trip.departure_time_in_sec) 
                    FROM Stop_Trip, Trip
                    WHERE Stop_Trip.stop_id = :in_stop_id 
                    AND Stop_Trip.trip_id = Trip.id
                    AND Trip.route_id = :in_trip_id
                    ;''', {'in_stop_id' : stop_id,
                        'in_trip_id' : route_id})
    result = cursor.fetchone()
    if result[0] is None:
        return ''
    else:
        return _seconds_to_str(result[0])


def get_earliest_service_for_stop_on_trip(db_filepath, route_id, 
        stop_id) -> str:
    '''If there is no match, returns an empty str'''
    db_connection = sqlite3.connect(db_filepath)
    cursor = db_connection.cursor()
    cursor.execute('''SELECT min(Stop_Trip.departure_time_in_sec)
                    FROM Stop_Trip, Trip
                    WHERE Stop_Trip.stop_id = :in_stop_id 
                    AND Stop_Trip.trip_id = Trip.id
                    AND Trip.route_id = :in_trip_id
                    ;''', {'in_stop_id' : stop_id,
                        'in_trip_id' : route_id})
    result = cursor.fetchone()
    if result[0] is None:
        return ''
    else:
        return _seconds_to_str(result[0])
