import unittest
import os
import sys

# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from py_dbutils.rdbms import postgres
from py_dbutils.rdbms import mysql, sqlite, mssql
from py_dbutils.rdbms import msaccess
import inspect
import os
import pprint
import logging as lg 

logging = lg.getLogger(__name__)

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
TEST_CSV_FILE = os.path.abspath(os.path.join(
    os.path.dirname(__file__), 'sample_data/unittest.csv'))
RDBMS = [postgres, mysql, mssql, sqlite]
PARAMS = [{'port': 55432},
          {'userid': 'root', 'port': 33306},
          {'userid': 'sa', 'port': 11433},
          {'file_path': os.path.join(TEST_OUTPUT_DIR, 'sqlite.db')}
          ]


class TestMsAccess(unittest.TestCase):

    def populate_test_table(self, DB, table_name=TEST_TABLE):
        import pandas as pd
        import os
        sql = """INSERT INTO {table} ({columns}) VALUES 
                {values}"""
        create_sql = "create table {table} ({columns})"
        dataframe = pd.read_csv(TEST_CSV_FILE, index_col=False)
        dataframe.col1 = 'asdfasdf"asdasdf'
        columns = ','.join(dataframe.columns)

        columns_values = ['{} CHAR'.format(c) for c in dataframe.columns]
        rows = []

        for key, value in dataframe.iterrows():
            x = []
            for c in value:
                x.append('"{}"'.format(c.replace('"', '""')))
            y = ','.join(x)
            rows.append('({})'.format(y))

        values = ',\n'.join(rows)
        DB.execute(create_sql.format(table=table_name,
                                     columns=','.join(columns_values)))
        DB.execute(sql.format(table=table_name,
                              columns=columns, values=values))

        # rows+="\n,("+(','.join(c.replace("'","''")) for c in row)+")"

    def test_msaccess(self):
        import pandas

        from shutil import copyfile
        src = os.path.join(curr_file_path, 'sample_data/AgeRange.mdb')
        dst = os.path.join(curr_file_path, TEST_OUTPUT_DIR, 'msaccess.mdb')
        copyfile(src, dst)

        x = msaccess.DB(dst,loglevel=logging.level)
        assert isinstance(x, msaccess.DB)
        # We don't want to put data into MsAccess we want to get away from access
        self.populate_test_table(DB=x, table_name='test')

        z = x.get_all_tables()
        y = x.get_table_columns(z[0])

        d, zz = x.query("select * from {}".format(z[0]))
        print(len(d))
        csv_file_path = os.path.join(
            curr_file_path, TEST_OUTPUT_DIR, 'test.csv')
        x.query_to_file(file_path=csv_file_path, sql='select * from tblEmployees',
                        file_format='CSV', header=True)

        with open(csv_file_path, 'r') as f:
            for line in f:
                print("Header line: 1")
                print(line)
                break
        x.query_to_file(file_path=csv_file_path, sql='select * from tblEmployees',
                        file_format='CSV', header=False)

        with open(csv_file_path, 'r') as f:
            for line in f:
                print("No Header line: 1")
                print(line)
                break

        test_header = ['employid', 'last_name', 'first " name', 'title', 'titile of, courtesey', 'dob',
                       'hire_date', 'addre', 'city', 'region', 'zipcode', 'country', 'Home_phone', 'ext', 'notes', 'reports_to']

        x.query_to_file(file_path=csv_file_path, sql='select * from tblEmployees', file_format='CSV',
                        header=test_header)

        with open(csv_file_path, 'r') as f:
            for line in f:
                print("My supplied headers line: 1")
                print(line)
                break

        # make sure we log error

        try:
            z = x.connect_SqlAlchemy()
        except Exception as e:
            print('Testing Exception Raised', e)
        parquet_file_path = os.path.join(
            curr_file_path, TEST_OUTPUT_DIR, 'test.parquet')
        x.query_to_file(parquet_file_path, 'select * from test',
                        file_format='PARQUET')
        print(pandas.read_parquet(parquet_file_path, engine='pyarrow'))

        hdf5_file_path = os.path.join(
            curr_file_path, TEST_OUTPUT_DIR, 'test.hdf5')
        x.query_to_file(hdf5_file_path, 'select * from test',
                        file_format='HDF5', hdf5_key='table')
        print(pandas.read_hdf(hdf5_file_path, 'table'))


if __name__ == '__main__':
    unittest.main()
