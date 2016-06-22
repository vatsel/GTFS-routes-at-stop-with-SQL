from os.path                    import exists
from sys                        import argv

from GTFSProcessor              import Routes   

EXACT_ARGS_NUM = 3
USAGE_STR = '''Usage: 
            routes_at_stop.py [database-Path] [stop_id]'''

if __name__ == '__main__':
    if len(argv) != EXACT_ARGS_NUM:
        print("Invalid arguments. %s" % USAGE_STR)
        exit()
    if not argv[1].endswith('.sqlite'):
        print("1st Argument must be a .sqlite database. %s" 
                % USAGE_STR)
        exit()
    if not exists(argv[1]):
        print("Database %s not found." % argv[1])
        exit()
    if Routes.check_if_stop_exists(argv[1],argv[2]):
        route_set = Routes.get_route_ids_passing_through_stop(argv[1], argv[2])
        print('Stop ID : %s' % argv[0])
        stop_name = Routes.get_stop_name(argv[1], argv[2])
        print('Stop Name : %s' % argv[0])
        if len(route_set) == 0:
            print('No routes found stopping at ID %d' % argv[2])
        else:
            print('Routes Stopping:')
            for route_id in route_set:
                route_short_name = Routes.get_route_short_name(
                        argv[1], route_id)
                route_long_name = Routes.get_route_long_name(
                        argv[1], route_id)
                earliest_dep = Routes.get_earliest_service_for_stop_on_trip(
                        argv[1], route_id, argv[2])
                latest_dep = Routes.get_latest_service_for_stop_on_trip(
                        argv[1], route_id, argv[2])
                print("%s - %s (earliest %s; latest %s)" % (route_short_name,
                    route_long_name, earliest_dep, latest_dep))
    else:
        print('Stop %s not found in database.' % argv[2])

