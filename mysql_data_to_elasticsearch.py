#! /usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: blinking.yan
# @Date:   2016-03-02 17:36:29
# @Last Modified by:   blinking.yan
# @Last Modified time: 2016-03-02 17:53:17
"""
migrate mysql data to elasticsearch
one database only each time
"""


from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch import helpers
from elasticsearch.exceptions import ConnectionError
import json
import MySQLdb
from MySQLdb import OperationalError
import MySQLdb.cursors
from itertools import izip
import sys
from datetime import datetime
reload(sys)
sys.setdefaultencoding('utf-8')


def get_string_columns(cursor, table_name):
    """ get mysql table string-like columns

    get mysql table column which type like '%char%', '%text%'.
        eg. char, varchar, text

    Arguments:
        cursor MySQLdb cursor --
        table_name string -- table name

    Returns:
        list -- column list
    """
    sql = "select column_name from information_schema.COLUMNS where table_name= %s and (data_type like %s or data_type like %s)"
    params = (table_name, '%char%', '%text%')
    cursor.execute(sql, params)
    results = cursor.fetchall()
    columns = [column for (column,) in results]
    return columns


def get_table_names(cursor):
    """get table names

    Arguments:
        cursor MySQLdb cursor

    Returns:
        list -- table name list
    """
    cursor.execute("SHOW TABLES")
    results = cursor.fetchall()
    table_names = [table_name for (table_name,) in results]
    return table_names


def get_row_count(cursor, table_name):
    """get table row count

    Arguments:
        cursor MySQLdb cursor --
        table_name string -- table name

    Returns:
        int -- row count
    """
    sql = "select count(*) from %s "

    cursor.execute(sql % table_name)

    results = cursor.fetchall()
    return results[0][0]


def get_actions(cursor, table_name, index):
    """get elasticsearch bulk actions

    see elasticsearch bulk api docs
        http://elasticsearch-py.readthedocs.org/en/master/helpers.html#elasticsearch.helpers.parallel_bulk
    Arguments:
        cursor MySQLdb cursor --
        table_name string -- table name
        index string -- elasticsearch index

    Yields:
        generator -- action
    """
    sql = 'select * from %s'
    cursor.execute(sql % table_name)
    # get table column name
    column_names = [i[0] for i in cursor.description]

    for row in cursor:
        action = {
            '_op_type': 'index',
            '_index': index,
            '_type': table_name,
        }
        data_dict = {}
        for key, value in izip(column_names, row):
            data_dict[key] = value
        action['_source'] = data_dict
        yield action

# mysql host
host = '192.168.16.168'
# mysql port
port = 3306
# mysql user
user = 'root'
# mysql passwd
passwd = 'root'
# mysql database
db = 'db_name'
# charset
charset = "utf8"
# elasticsearch url
url = 'http://%s:9200/' % host
# parallel bulk thread count
thread_count = 10
# document count for each bulk request
chunk_size = 1000
try:
    # connect
    mysqldb = MySQLdb.connect(host=host, port=port, user=user,
                              passwd=passwd, db=db, charset=charset, cursorclass=MySQLdb.cursors.SSCursor)
    # get db cursor
    cursor = mysqldb.cursor()

except OperationalError, e:
    print "error code: %i, error info: %s" % (e[0], e[1])
    sys.exit(1)

# get table names
table_names = get_table_names(cursor)

total_table = len(table_names)
print("total table count: %i" % total_table)
print("----------------------------------------------")
# es client
client = Elasticsearch(url, connection_class=RequestsHttpConnection)
# record count of all tables
migrate_total_count = 0
# fail record count of all tables
migrate_fail_count = 0
start_time = datetime.now()
failed_table_names = []
# check index exists
if not client.indices.exists(index=db):
    client.indices.create(index=db)

for index, table_name in enumerate(table_names, 1):

    print("%i. migrating  table: %s" % (index, table_name))
    row_count = get_row_count(cursor, table_name)
    migrate_total_count += row_count
    print("row count: %i" % row_count)

    columns = get_string_columns(cursor, table_name)
    # put index type mapping
    properties = {}
    for column in columns:
        properties[column] = {"type": "string"}
    mapping = {table_name: {"properties": properties}}
    # print json.dumps(mapping)
    # if put error, see
    # https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-put-mapping.html#merging-conflicts
    try:
        client.indices.put_mapping(
            index=db, doc_type=table_name, body=json.dumps(mapping), update_all_types=True)
    except Exception, e:
        print(
            "put mapping failed, maybe error u'illegal_argument_exception', u'mapper [xxxx] cannot be changed from type [long] to [string]'")

    try:
        # get bulk actions
        actions = get_actions(cursor=cursor, table_name=table_name, index=db)

        failed_count = 0
        responses = helpers.parallel_bulk(
            client=client, actions=actions, thread_count=thread_count, chunk_size=chunk_size)
        for success, msg in responses:
            status = msg['create']['status']
            if not success or status != 201:
                failed_count += 1
        if failed_count > 0:
            print("----------------> failed records %i" % failed_count)
        else:
            print("migrate success")
        migrate_fail_count += failed_count
    except ConnectionError, e:
        print e
        sys.exit(1)
    except Exception, e:
        print "migrate table: %s error" % table_name, type(e)
        # print e
        failed_table_names.append(table_name)
    # time.sleep(1)
    # if index > 20:
    #     break
    print("----------------------------------------------")

end_time = datetime.now()
failed_table = len(failed_table_names)
success_table = total_table - failed_table
print("total table: %i,  success: %i, failed: %i" %
      (total_table, success_table, failed_table))
print("failed table_name: %s" % str(failed_table_names))
print(
    "see https://www.elastic.co/guide/en/elasticsearch/hadoop/current/mapping.html#_incorrect_mapping for details")
print("migrate_total_count: %i, migrate_fail_count: %i" %
      (migrate_total_count, migrate_fail_count))
print("start_time: %s, end_time: %s, cost_time: %s" %
      (str(start_time), str(end_time), str(end_time - start_time)))
