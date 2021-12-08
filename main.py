import datetime
import json
import psycopg2
from flask import Flask, request, make_response
from databaseconnection import DatabaseConnection, mongodb_connection

app = Flask(__name__)


@app.route('/post', methods=['POST'])
def internal_authentication():
    key = request.headers.get('API-Key')

    cursor = mongodb_connection(key, "mongodb")

    if cursor is not None and cursor["apiKey"] == key:
        if cursor["expiry"] > datetime.datetime.now():
            userid = cursor["userId"]
            accountid = cursor["accountId"]

            json_body = dict(request.json)

            try:
                connection = DatabaseConnection.get_connection('database')
                cursor_postgres = connection.cursor()
                cursor_postgres.execute(
                    "insert into transaction_push (user_id, exchange_id, transaction_json, date) values (" + "'" + str(
                        userid) + "',"
                                  "'" + str(accountid) + "','" + json.dumps(json_body) + "','" + str(
                        datetime.datetime.now()) + "') ON CONFLICT DO NOTHING")
                connection.commit()

                if connection:
                    connection.close()

                return make_response('Transaction stored', 201, {"response": "Success"})
            except psycopg2.Error as db_err:
                print('Exception occurred {}', db_err.__str__())
                return make_response('Error inserting data (Postgres): ', 500, {'response': 'Invalid token!'})
            except ConnectionError as err:
                print('Exception occurred {}', err.__str__())
                return make_response('Error connecting Postgres:', 500, {'response': 'Invalid token!'})
        else:
            return make_response('Token expired:', 401, {'response': 'Token expired'})

    return make_response('Unauthorized', 401, {'response': 'Unauthorized'})


if __name__ == '__main__':
    app.run(debug=True)
