import re
import os
import urllib.request as req
import json
import time
import sqlite3

from flask import Flask
from flask_restplus import Resource, Api

app = Flask(__name__)
api = Api(app, title="COMP9321", description="Assignment-2")


#############
def database_commander(db, cmd):
    db_connection = sqlite3.connect(db)
    cursor = db_connection.cursor()
    if len(re.findall(';', cmd)) > 1:
        cursor.executescript(cmd)
    else:
        cursor.execute(cmd)
    res = cursor.fetchall()
    db_connection.commit()
    db_connection.close()
    return res


# database initialization
def Initialize_db(database_file):
    if os.path.exists(database_file):
        print('Oops!, the specified database has already been established.')
        return False
    print('Currently database is creating...')
    collection = '_'
    database_commander(database_file,
                       'CREATE TABLE Collection('
                       'collection_id INTEGER UNIQUE NOT NULL,'
                       'collection_name VARCHAR(100),'
                       'indicator VARCHAR(100),'
                       'indicator_value VARCHAR(100),'
                       'creation_time DATE,'
                       'CONSTRAINT collection_pkey PRIMARY KEY (collection_id));'
                       +
                       'CREATE TABLE Entries('
                       'id INTEGER NOT NULL,'
                       'country VARCHAR(100),'
                       'date VARCHAR(10),'
                       'value VARCHAR(100),'
                       'CONSTRAINT entry_fkey FOREIGN KEY (id) REFERENCES Collection(collection_id));'
                       )
    return 1


def handle_request(indicator, nb_of_page, start=2012, end=2017, format='json'):
    url = f'http://api.worldbank.org/v2/countries/all/indicators/' + \
          f'{indicator}?date={start}:{end}&format={format}&pages={nb_of_page}'
    source = req.Request(url)
    data = req.urlopen(source).read()
    if re.findall('Invalid value', str(data), flags=re.I):
        return False
    return json.loads(data)[1]


def retrieve_format_json(collection_query, entries_query):
    result = {"id": "{}".format(collection_query[0]),
              "indicator": "{}".format(collection_query[2]),
              "indicator_value": "{}".format(collection_query[3]),
              "creation_time": "{}".format(collection_query[4]),
              "entries": []
              }
    for i in range(len(entries_query)):
        result["entries"].append({"country": entries_query[i][0],
                                  "date": entries_query[i][1],
                                  "value": entries_query[i][2]
                                  })
    return result


def request_tool(database, collection, action, **kwargs):
    if action == 'post':
        return post_tool(database, collection, kwargs['indicator'])
    elif action == 'delete':
        return delete_tool(database, collection, kwargs['collection_id'])
    elif action == 'getall':
        return get_tool(database, collection, 'getall')
    elif action == 'getone':
        return get_tool(database, collection, 'getone', collection_id=kwargs['collection_id'])
    elif action == 'getoneyc':
        return get_tool(database, collection, 'getoneyc', collection_id=kwargs['collection_id'],
                        year=kwargs['year'], country=kwargs['country'])
    elif action == 'gettopbottom':
        top_test = re.search("^(\+)(\d+)$", kwargs['query'])
        bottom_test = re.search("^(\-)(\d+)$", kwargs['query'])
        if top_test:
            return get_tool(database, collection, 'gettopbottom', collection_id=kwargs['collection_id'],
                            year=kwargs['year'], flag='top', value=top_test.group(2))
        if bottom_test:
            return get_tool(database, collection, 'gettopbottom', collection_id=kwargs['collection_id'],
                            year=kwargs['year'], flag='bottom', value=bottom_test.group(2))
        else:
            return {"message":
                        "Your input arguments are not in correct format! Must be either top<int> or bottom<int>."}, 400


# Q3-Q6
def get_tool(db, collection, cmd, **kwargs):
    # Q3
    def collect_json_format(query):
        return {"url": "/{}/{}".format(query[1], query[0]),
                "id": "{}".format(query[0]),
                "creation_time": "{}".format(query[4]),
                "indicator": "{}".format(query[2])
                }
    if cmd == 'getall':
        query = database_commander(db, f"SELECT * FROM Collection WHERE collection_name ='{collection}';")
        if query:
            res = list()
            count = 0
            for i in range(len(query)):
                count += 1
                res.append(collect_json_format(query[i]))
            return res, 200
        return {"message": f"The collection '{collection}' not found in data source!"}, 404

    # Q4
    if cmd == 'getone':
        collection_query = database_commander(db,
                                              f"SELECT * "
                                              f"FROM Collection "
                                              f"WHERE collection_name = '{collection}'"
                                              f"AND collection_id = {kwargs['collection_id']};")

        entries_query = database_commander(db,
                                           f"SELECT country, date, value "
                                           f"FROM Entries "
                                           f"WHERE id = {kwargs['collection_id']};")
        if collection_query:
            return retrieve_format_json(collection_query[0], entries_query), 200
        return {"message":
                    f"The collection '{collection}' with id {kwargs['collection_id']} not found in data source!"}, 404

    # Q5
    if cmd == 'getoneyc':
        join_query = database_commander(db,
                                        f"SELECT collection_id, indicator, country, date, value "
                                        f"FROM Collection "
                                        f"JOIN Entries ON (Collection.collection_id = Entries.id) "
                                        f"WHERE collection_id = {kwargs['collection_id']} "
                                        f"AND date = '{kwargs['year']}' "
                                        f"AND country = '{kwargs['country']}';")
        if join_query:
            return {"collection_id": "{}".format(join_query[0][0]),
                    "indicator": "{}".format(join_query[0][1]),
                    "country": "{}".format(join_query[0][2]),
                    "year": "{}".format(join_query[0][3]),
                    "value": "{}".format(join_query[0][4])
                    }, 200
        return {"message":
                    f"The given arguments collections = '{collection}', {kwargs} not found in data source!"}, 404

    # Q6
    if cmd == 'gettopbottom':
        insert_flag = ''
        if kwargs['flag'] == '+':
            insert_flag = 'DESC'

        collection_query = database_commander(db,
                                              f"SELECT * FROM Collection WHERE collection_name = '{collection}'"
                                              f"AND collection_id = {kwargs['collection_id']};")


        entries_query = database_commander(db,
                                           f"SELECT country, date, value "
                                           f"FROM Entries "
                                           f"WHERE id = {kwargs['collection_id']} "
                                           f"AND date = '{kwargs['year']}' "
                                           f"AND value != 'None' "
                                           f"GROUP BY country, date, value "
                                           f"ORDER BY CAST(value AS REAL) {insert_flag} "
                                           f"LIMIT {kwargs['value']};")

        if collection_query:
            result_dict = retrieve_format_json(collection_query[0], entries_query)
            result_dict.pop("collection_id")
            result_dict.pop("creation_time")
            return result_dict, 200
        return {"message":
                    f"No data matches !"}, 404


def post_tool(db, collection, indicator):
    def collect_json_format(query):
        return {"url": "/{}/{}".format(query[1], query[0]),
                "id": "{}".format(query[0]),
                "creation_time": "{}".format(query[4]),
                "indicator": "{}".format(query[2])
                }
    query = database_commander(db, f"SELECT * FROM Collection WHERE indicator = '{indicator}';")
    if query:
        return collect_json_format(query[0]), 200

    if not query:
        data_first_page = handle_request(indicator, 1)
        data_second_page = handle_request(indicator, 2)
        if not data_first_page or not data_second_page:
            return {"message": f"The indicator '{indicator}' not found in data source!"}, 404
        new_id = re.findall('\d+', str(database_commander(db, 'SELECT MAX(collection_id) FROM Collection;')))
        if not new_id:
            new_id = 1
        else:
            new_id = int(new_id[0]) + 1
        update_table_of_collection(db, new_id, collection, data_first_page)
        update_table_of_entry(db, new_id, data_first_page)
        update_table_of_entry(db, new_id, data_second_page)
        new_query = database_commander(db, f"SELECT * FROM Collection WHERE indicator = '{indicator}';")
        return collect_json_format(new_query[0]), 201


def update_table_of_collection(db, given_id, given_collection_name, data):
    current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    collections = "INSERT INTO Collection VALUES ({}, '{}', '{}', '{}', '{}');" \
        .format(given_id, given_collection_name, data[0]['indicator']['id'], data[0]['indicator']['value'],
                current_time)
    database_commander(db, collections)


def delete_tool(db, collection, collection_id):
    query = database_commander(db,
                               f"SELECT * FROM Collection WHERE collection_name = '{collection}' "
                               f"AND collection_id = {collection_id};")
    if not query:
        return {"message": f"Oops! Collection {collection_id} NOT FOUND in the database!"}, 404
    else:
        database_commander(db, f"DELETE FROM Entries WHERE id = {collection_id};")
        database_commander(db, f"DELETE FROM Collection WHERE collection_id = '{collection_id}';")
        return {"message": f"Collection {collection_id} is successfully removed from the database!",
                "id": collection_id}, 200




def update_table_of_entry(db, given_id, data):
    entry = "INSERT INTO Entries VALUES"
    for item in data:
        entry += f"({given_id}, '{item['country']['value']}', '{item['date']}', '{item['value']}'),"
    entry = entry.rstrip(',') + ';'
    database_commander(db, entry)


parser = api.parser()
parser.add_argument('indicator_id', type=str, help='For Q1 use only.', location='args')
parser.add_argument('order_by', type=str, help='For Q3 use only', location='args')
parser.add_argument('q', type=str, help='Your query here (e.g."+N"), for Q6 use only', location='args')


# Q1 & Q3,
@api.route("/collections")
@api.response(200, 'OK')
@api.response(400, 'Non-valid Request')
@api.response(404, 'Not Found')
@api.response(201, 'Successfully Created')
class SingleRoute(Resource):
    @api.doc(params={'indicator_id': 'indicator_id'})
    def post(self):
        print(parser.parse_args())
        query = parser.parse_args()['indicator_id']
        if not query:
            return {
                       "message": "Please check weather the indicatorID is given!"
                   }, 400
        return request_tool('z5243425.db', 'collections', 'post', indicator=query)

    @api.doc(params={'order_by': 'ordering'})
    def get(self):
        query = parser.parse_args()['order_by']
        unsort_json = request_tool('z5243425.db', 'collections', 'getall')
        pure_unsort_json = unsort_json[0]
        commend_list = query.split(",")
        for i in commend_list:
            if i[0] == '+':
                pure_unsort_json = sorted(pure_unsort_json, key=lambda j: j[i[1:]], reverse=False)
            else:
                pure_unsort_json = sorted(pure_unsort_json, key=lambda j: j[i[1:]], reverse=True)
        return pure_unsort_json, 200


# Q2 & Q4
@api.route("/collections/<int:collection_id>")
@api.response(200, 'OK')
@api.response(400, 'Non-valid Request')
@api.response(404, 'Not Found')
class DualRoute(Resource):
    def delete(self, collection_id):
        return request_tool('z5243425.db', 'collections', 'delete', collection_id=collection_id)

    def get(self, collection_id):
        return request_tool('z5243425.db', 'collections', 'getone', collection_id=collection_id)


# Q5
@api.route("/collections/<int:collection_id>/<int:year>/<string:country>")
@api.response(200, 'OK')
@api.response(400, 'Non-Valid Request')
@api.response(404, 'Not Found')
class QuadRoute(Resource):
    def get(self, collection_id, year, country):
        return request_tool('z5243425.db', 'collections', 'getoneyc', collection_id=collection_id,
                            year=year, country=country)


# Q6

@api.route("/collections/<int:collection_id>/<int:year>")
@api.doc(parser=parser)
@api.response(200, 'OK')
@api.response(400, 'Non-valid Request')
@api.response(404, 'Not Found')
class ArgsRoute(Resource):
    def get(self, collection_id, year):
        query = parser.parse_args()['q']
        return request_tool('z5243425.db', 'collections', 'gettopbottom', collection_id=collection_id,
                            year=year, query=query)


if __name__ == "__main__":
    Initialize_db('z5243425.db')
    app.run(host='127.0.0.1', port=9996, debug=True)
