Overview
==============================================
An exercise processing GTFS data using Python & SQLite, with a focus on functional programming solution.


Usage
===============================================
python import.py [gtfs-zip-archive.path] [target-database-path]
e.g. 
python import.py citymapper-coding-test.gtfs.zip db.sqlite

python route_at_stop.py [target-database-path] [stop_id]
e.g. 
python route_at_stop.py db.sqlite 5644

python tests.py 
* requires a "db.sqlite" file in the same directory (generated in import process)



Requirements
===============================================
- Python 3 (No additional requirements)
- GTFS data with stop_routes.txt, routes.txt, trip_id.txt, route_id, stop_id.txt
	(A working SQLite database is already included)


Operational Overview
==============================================
//// Import
1 Create Tables for Routes, Trips and Stops.
2 Crete linking tables Stops<->Trips, Stops<->Routes.
* Departure_time is saved in seconds from midnight, since SQLite's time() function evaluates 24:01:00 as larger than 25:00:00

//// Routes at Stop
1 Using Stops<->Routes table, get a set of route_ids passing through the stops.
2 Using Stop table, get stop name.
3 foreach route_id:
	3a Get route's short name from Route table.
	3b Get route's long name from Route table.
	3c Get route's earliest departure time from Route table.
	3d Get route's latest departure time from Route table.



Assumptions
================================================
- tests.py is run after import.py generates a database with name "db.sqlite" in the same directory.

//// Input
- route_id & stop_id are integers.
- stop_routes.txt's departure_time column is in HH:MM::SS format.
- routes.txt short_names do not exceed 10 characters.
- routes.txt long_names do not exceed 150 characters.
- trip_id s do not exceed 100 characters.



Functionality interpreted as out of scope
==================================================
- Updating the database with any new GTFS data, the database only gets created and populated once.
- Model classes & instances. This also makes the program more functional.
