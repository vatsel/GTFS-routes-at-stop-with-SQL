from os.path                            import exists
from sys                                import argv
from GTFSProcessor                      import ZIPImporter


EXACT_ARGS_NUM = 3
USAGE_STR = '''Usage: 
            import.py [input-data-path] [database-Path]'''

if __name__ == '__main__':
    if len(argv) != EXACT_ARGS_NUM:
        print("Invalid arguments. %s" % USAGE_STR)
        exit()
    if not argv[1].endswith('.zip'):
        print("1st Argument must be a .zip archive. %s" % USAGE_STR)
        exit()
    if not exists(argv[1]):
        print("Archive %s not found." % argv[1])
        exit()
    if exists(argv[2]):
        while True:
            in_ = input("Database %s already exists, overwrite?(y/n)"
                % argv[2]).lower()
            if in_ == 'n':
                print('Aborting.')
                exit()
            elif in_ == 'y':
                break
                
    ZIPImporter.import_into_database(argv[1],argv[2])
    
