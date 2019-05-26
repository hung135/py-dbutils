import unittest
import os
import sys

# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
 
from py_dbutils.rdbms import  sqlite
 
import inspect
import os
import pprint

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
RDBMS = [sqlite]
PARAMS = [
          {'file_path': os.path.join(TEST_OUTPUT_DIR, 'sqlite.db')}
          ]


class TestSqlLite(unittest.TestCase):

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

    def test_sqlite(self):
        import pandas
        
        dst = os.path.join(curr_file_path, TEST_OUTPUT_DIR, 'sqlite.db')
        
        #connects to or creates an empty sqlite db
        x = sqlite.DB(dst)
        assert isinstance(x, sqlite.DB)
        
        self.populate_test_table(DB=x, table_name=TEST_TABLE_NAME)

        z = x.get_all_tables()
        y = x.get_table_columns(z[0])

        d, _ = x.query("select * from {}".format(z[0]))
        
        csv_file_path = os.path.join(
            curr_file_path, TEST_OUTPUT_DIR, 'test.csv')
        x.query_to_file(file_path=csv_file_path, sql='select * from {}'.format(TEST_TABLE_NAME),
                        file_format='CSV', header=True)

        with open(csv_file_path, 'r') as f:
            for line in f:
                print("Header line: 1")
                print(line)
                break
        x.query_to_file(file_path=csv_file_path, sql='select * from {}'.format(TEST_TABLE_NAME),
                        file_format='CSV', header=False)

        with open(csv_file_path, 'r') as f:
            for line in f:
                print("No Header line: 1")
                print(line)
                break

        test_header = ['column1','column2','column3']

        x.query_to_file(file_path=csv_file_path, sql='select * from {}'.format(TEST_TABLE_NAME),
                        file_format='CSV', header=test_header)

        with open(csv_file_path, 'r') as f:
            for line in f:
                print("My supplied headers line: 1")
                print(line)
                break

        # make sure we log error

        #Test sqlalchemy
        z = x.connect_SqlAlchemy()
        print("SQL Alchemy Works")

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
