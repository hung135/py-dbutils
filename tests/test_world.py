import unittest
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from py_dbutils.rdbms import postgres
from py_dbutils.rdbms import mysql
import inspect
import os

DBSCHEMA = 'postgres'
COMMIT = True
PASSWORD = 'docker'
USERID = 'postgres'
HOST = 'localhost'
PORT = '5432'
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
        x = postgres.DB();
        print(x)
        self.populate_test_table(x)

    def test_mysql(self):
        x = mysql.DB(userid='root');
        print(x)
        self.populate_test_table(x)

    # def test_sqlalch(self):
    #     x = postgres.DB();
    #     x.connect_SqlAlchemy()
    #     x = mysql.DB();
    #     x.connect_SqlAlchemy()


if __name__ == '__main__':
    unittest.main()
