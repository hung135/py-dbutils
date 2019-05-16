import unittest
import os
import sys

#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from py_dbutils.rdbms import postgres
from py_dbutils.rdbms import mysql, sqlite , mssql
from py_dbutils.rdbms import msaccess
import inspect
import os
import pprint

DBSCHEMA = 'postgres'
COMMIT = True
PASSWORD = os.getenv('PGPASSWORD', None) or 'docker'
USERID = 'postgres'
   
HOST = os.getenv('PGHOST', None) or 'localhost'
PORT = os.getenv('PGPORT', None) or '5432'
DATABASE = os.getenv('PGDATABASE', None) or 'postgres'
DBTYPE = 'POSTGRES'
APPNAME = 'test_connection'
print("--------",HOST)

TEST_OUTPUT_DIR = "_testoutput"
curr_file_path = os.path.join(os.path.dirname(__file__))
if not os.path.exists(os.path.join(curr_file_path, TEST_OUTPUT_DIR)):
    os.makedirs(os.path.join(curr_file_path, TEST_OUTPUT_DIR))

TEST_SCHEMA = 'test'
TEST_TABLE_NAME = 'test'
TEST_TABLE = '{}.test'.format(TEST_SCHEMA)
TEST_CSV_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), 'sample_data/unittest.csv'))
RDBMS = [postgres, mysql, mssql, sqlite]
PARAMS = [{'port': PORT},
          {'userid': 'root', 'port': 33306},
          {'userid': 'sa', 'port': 11433},
          {'file_path': os.path.join(TEST_OUTPUT_DIR, 'sqlite.db')}
          ]


class TestDB(unittest.TestCase):
    def clean_test_db(self, DB):
        DB.execute('drop schema {} cascade'.format(TEST_SCHEMA))
        DB.commit()
    def populate_test_table(self, DB, table_name=TEST_TABLE):
        import pandas as pd
        import os

        dataframe = pd.read_csv(TEST_CSV_FILE)
        DB.execute('create schema {}'.format(TEST_SCHEMA))
      
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
     
    def test_postgres(self):
        
        x = postgres.DB(port=PORT,pwd='postgres')
        self.clean_test_db(x)
        try:
            fail_db = postgres.DB() #purpose fail
        except Exception as e:
            print("Purposely Fail test Destroy",e)
        
        self.populate_test_table(x)
        x.execute("truncate table test.test")
        x.commit()
        x_cols=x.get_table_columns('test.test')
        print("-----------------cols",x_cols,x.get_a_row('select count(*) from test.test'))
        z, = (x.get_a_row('select 1 as col1   '))



    @unittest.skip
    def test_sqlite(self):
        import pandas
        x = sqlite.DB(os.path.join(curr_file_path, TEST_OUTPUT_DIR, 'test_sqlite.db'));
        assert isinstance(x, sqlite.DB)

        self.populate_test_table(DB=x, table_name='test')
        z, = (x.get_a_row('select 1 as col1   '))
        parquet_file_path=os.path.join(curr_file_path, TEST_OUTPUT_DIR, 'test.parquet')
        x.query_to_parquet(parquet_file_path,'select * from test')
        print(pandas.read_parquet(parquet_file_path, engine='pyarrow'))


        csv_file_path=os.path.join(curr_file_path, TEST_OUTPUT_DIR, 'test.csv')
        x.query_to_csv(csv_file_path,'select * from test')
        print(pandas.read_csv(csv_file_path))


        hdf5_file_path=os.path.join(curr_file_path, TEST_OUTPUT_DIR, 'test.hdf5')
        x.query_to_hdf5(hdf5_file_path,'select * from test')
        print(pandas.read_hdf(hdf5_file_path,'table'))

    @unittest.skip
    def test_all(self):
        for db, params in zip(RDBMS, PARAMS):
            print("------", db)
            x = db.DB(**params)
            x.execute(sql="create schema {};".format(TEST_SCHEMA))
            x.commit()
            self.populate_test_table(x)


if __name__ == '__main__':
    unittest.main()
