import unittest
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from py_dbutils.rdbms import postgres
from py_dbutils.rdbms import mysql, mssql,sqlite
import inspect
import os

DBSCHEMA = 'postgres'
COMMIT = True
PASSWORD = 'docker'
USERID = 'postgres'
HOST = 'localhost'
PORT = '55432'
DATABASE = 'postgres'
DBTYPE = 'POSTGRES'
APPNAME = 'test_connection'

TEST_OUTPUT_DIR = "_testoutput"
curr_file_path = os.path.join(os.path.dirname(__file__))
if not os.path.exists(os.path.join(curr_file_path, TEST_OUTPUT_DIR)):
    os.makedirs(os.path.join(curr_file_path, TEST_OUTPUT_DIR))

TEST_SCHEMA = 'test'
TEST_TABLE_NAME = 'test'
TEST_TABLE = '{}.test'.format(TEST_SCHEMA)
TEST_CSV_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), 'sample_data/unittest.csv'))
RDBMS = [postgres, mysql, mssql,sqlite]
PARAMS = [{'port': 55432},
          {'userid': 'root', 'port': 33306},
          {'userid': 'sa', 'port': 11433},
          {'file_path':os.path.join(TEST_OUTPUT_DIR,'sqlite.db')}
          ]


class TestDB(unittest.TestCase):

    def populate_test_table(self, DB):
        import pandas as pd
        import os

        dataframe = pd.read_csv(TEST_CSV_FILE)

        engine = DB.connect_SqlAlchemy()

        dataframe.to_sql(name=TEST_TABLE.split('.')[-1], con=engine, index=False, if_exists='replace',
                         schema=TEST_SCHEMA)

        print("Loaded Test Data")
        print(DB.query("select * from {}".format(TEST_TABLE)))

    def test_postgres(self):
        x = postgres.DB(port=55432);

        z, = (x.get_a_row('select 1 as col1   '))

    # def test_mysql(self):
    #     x = mysql.DB(userid='root',port=33306);
    #
    #     x.execute(sql="create schema {};".format(TEST_SCHEMA))
    #     x.commit()
    #     self.populate_test_table(x)

    def test_all(self):
        for db, params in zip(RDBMS, PARAMS):
            print("------", db)
            x = db.DB(**params)
            x.execute(sql="create schema {};".format(TEST_SCHEMA))
            x.commit()
            self.populate_test_table(x)


if __name__ == '__main__':
    unittest.main()
