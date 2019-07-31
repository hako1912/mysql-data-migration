import yaml
import pymysql.cursors
import os
import sys

scriptDir = os.path.dirname(os.path.realpath(__file__))

if(len(sys.argv) < 2):
    print('config file is required.')
    exit(1)
configFileName = sys.argv[1]

# load config file
config = {}
with open(f"{scriptDir}/{configFileName}") as f:
    config = yaml.safe_load(f)

# to convert datetime fields to string
conv = pymysql.converters.conversions.copy()
conv[10] = str  # date
conv[11] = str  # time
conv[12] = str  # datetime

sourceConnection = pymysql.connect(host=config['from']['host'],
                                   user=config['from']['user'],
                                   port=config['from']['port'],
                                   password=config['from']['password'],
                                   charset='utf8mb4',
                                   cursorclass=pymysql.cursors.DictCursor,
                                   conv=conv)

distConnection = pymysql.connect(host=config['to']['host'],
                                 user=config['to']['user'],
                                 port=config['to']['port'],
                                 password=config['to']['password'],
                                 db=config['to']['db'],
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor,
                                 conv=conv)

print('Start migration.')
print('----------')
successCount = 0
# errorCount = 0
try:
    with distConnection.cursor() as distCursor:
        with sourceConnection.cursor() as cursor:
            cursor.execute(config['from']['query'])
            result = cursor.fetchone()

            while result is not None:
                columns = list(result.keys())
                placeHolders = ', '.join(list(map(lambda x: '%s', columns)))
                sql = f"INSERT INTO {config['to']['table']} ({', '.join(columns)}) VALUES ({placeHolders})"
                try:
                    # execute INSERT statement
                    distCursor.execute(sql, list(result.values()))
                    successCount += 1

                # skip duplicated id error
                # except pymysql.err.IntegrityError as ex:
                #     errorCount += 1
                #     print(ex, f"{list(result.values())}")
                result = cursor.fetchone()
        distConnection.commit()
finally:
    sourceConnection.close()
    distConnection.close()

print('----------')
print(f"Added rows: {successCount}")
# print(f"Error rows: {errorCount}")
print('Done.')
