==========
py-dbutils
==========
Last build status |ImageLink|_

.. |ImageLink| image:: https://travis-ci.org/hung135/py-dbutils.svg?branch=dev
.. _ImageLink: hhttps://travis-ci.org/hung135/py-dbutils

Python Wrapper to interface with various databases.
Connects to multiple databases to use for data pipelines and etl
 

Description
===========
 
Forget about boiler plate code to setup and tear database connections:
This library will give you all the basic methods to pull and push data from various types of database
Initial release will support Postgres and Mysql.
I will focus on python 3.7 just to discourage people from continuing the python 2.7 train.
Move to 3.6+ and get learn something new.

This package wraps high level calls needed to interface with a database.
Get the programmer out of worrying about how to connect to and manage a database connection.
Start using it.

Pypi builds:
https://pypi.org/project/py-dbutils/

pip install py_dbtuils
Usage:

import py_dbutils.rdbms.postgres as db_utils
logging = lg.getLogger()
db=db_utils.DB(schema=dbschema
                                ,label='label_for_dbconnection'
                                ,dbname='your_db_name'
                                ,dbtype='POSTGRES'
                                ,host='hostname'
                                ,port='5432'
                                ,pwd='xxxxxxx'
                                ,userid='db_userid'
                                ,loglevel=logging.level
                                )

#run a sql 
db.execute("update table set xxx='yyy' where xxx=1')
#create a table from your dataframe 
db.create_table_from_dataframe(dataframe, 'schema_name.table_name')

#this will dump your dataframe to stdint and pipe it thru postgres copy command FASST!
#require psycopg2-binary
rows_inserted = db.bulk_load_dataframe(dataframe, table_name_fqn='schema_name.table_name')

#sometimes you just need a value from a table
v=db.get_a_value("select count(*) from some_schema.some_table)
#sometimes you just want a specific row 
row, meta=db.get_a_row("select * from some_schema.some_table where id=12345)

Other functions...not all work are here:
https://github.com/hung135/py-dbutils/blob/master/src/py_dbutils/parents.py

Currently Mostly supports:
postgres

WIP if ever:
mysql
mssql
msaccess
sqlite

NB:
Install jaydebeapi if you need to mess with JAVA JDBC
 

 
