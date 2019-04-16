import unittest
import os
import sys

# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from py_dbutils.rdbms import mysql
import inspect
import os
import pprint

DBSCHEMA = 'mysql'
COMMIT = True
PASSWORD = 'docker'
USERID = 'root'
HOST = 'localhost'
PORT = '33306'
DATABASE = 'mysql'

APPNAME = 'test_connection'

TEST_OUTPUT_DIR = "_testoutput"
curr_file_path = os.path.join(os.path.dirname(__file__))
if not os.path.exists(os.path.join(curr_file_path, TEST_OUTPUT_DIR)):
    os.makedirs(os.path.join(curr_file_path, TEST_OUTPUT_DIR))

TEST_SCHEMA = 'test'
TEST_TABLE_NAME = 'test'
TEST_TABLE = '{}.test'.format(TEST_SCHEMA)
TEST_CSV_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), 'sample_data/unittest.csv'))
RDBMS = [mysql]
PARAMS = [{'port': 55432}
          ]


class TestMysql(unittest.TestCase):

    def populate_test_table(self, DB, table_name=TEST_TABLE):
        import pandas as pd
        import os

        dataframe = pd.read_csv(TEST_CSV_FILE)

        engine = DB.connect_SqlAlchemy()
        table_split = table_name.split('.')

        schema = None
        table = table_name
        if len(table_split) > 1:
            table = table_split[-1]
            schema = table_split[0]

        dataframe.to_sql(name=table, con=engine, index=False, if_exists='replace', schema=schema)

        print("Loaded Test Data")
        print(DB.query("select * from {}".format(table_name)))

    def test_mysql(self):
        import pandas

        from shutil import copyfile
        src = os.path.join(curr_file_path, 'sample_data/AgeRange.mdb')
        dst = os.path.join(curr_file_path, TEST_OUTPUT_DIR, 'msaccess.mdb')
        copyfile(src, dst)

        x = mysql.DB(port=PORT, userid=USERID, host=HOST);
        assert isinstance(x, mysql.DB)
        # We don't want to put data into MsAccess we want to get away from access
        self.populate_test_table(DB=x, table_name='test')

        z = x.get_all_tables()

        print(z)
        y = x.get_table_columns('mysql.test')
        # make sure we log error
        z = x.connect_SqlAlchemy()

        print(y)
        file = os.path.join(curr_file_path, TEST_OUTPUT_DIR, 'test_mysql.csv')
        x.query_to_file(file, 'select * from test', header=y, file_format='CSV')
        print(pandas.read_csv(file))

        file = os.path.join(curr_file_path, TEST_OUTPUT_DIR, 'test.parquet')
        x.query_to_file(file, 'select * from test', file_format='PARQUET')
        print(pandas.read_parquet(file, engine='pyarrow'))

        file = os.path.join(curr_file_path, TEST_OUTPUT_DIR, 'test.hdf5')
        x.query_to_file(file, 'select * from test', file_format='HDF5', hdf5_key='table')
        print(pandas.read_hdf(file, 'table'))


if __name__ == '__main__':
    unittest.main()
