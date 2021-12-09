import psycopg2
from flask_pymongo import MongoClient
from psycopg2 import Error, DataError
from configuration import Configuration

Configuration.init_config('config.ini')


class DatabaseConnection:
    def __init__(self):
        pass

    @staticmethod
    def get_connection(section):
        host = Configuration.get(section, 'host')
        port = Configuration.get(section, 'port')
        db_name = Configuration.get(section, 'db_name')
        user = Configuration.get(section, 'user')
        password = Configuration.get(section, 'password')
        try:
            connection = psycopg2.connect(user=user, password=password, host=host, port=port,
                                          database=db_name)
            return connection
        except (Exception, Error, DataError) as error:
            print('Error while connecting to PostgreSQL', error)


def mongodb_connection(key, section_mongodb):
    connection_string = "mongodb://grey:greywolf@101.53.132.174:5435/catax?authSource=catax&readPreference=primary" \
                        "&appname=MongoDB%20Compass&directConnection=true&ssl=false "
    client = MongoClient(connection_string)
    db = client.get_database(Configuration.get(section_mongodb, 'dbname'))

    collection = db.get_collection(Configuration.get(section_mongodb, 'collection_name'))
    cursor = collection.find_one({"apiKey": key})

    return cursor
