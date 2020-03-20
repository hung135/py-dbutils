import unittest
import os
import sys

# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from py_dbutils.rdbms import mssql
import inspect
import os
import pprint
import logging as lg 

logging = lg.getLogger()

APPNAME = 'test_connection'

DBSCHEMA = 'test'
COMMIT = True
PASSWORD = os.getenv('MS_PASSWORD', None) or 'docker'
USERID = os.getenv('MS_USER', None) or 'sa'
HOST = os.getenv('MS_HOST', None) or 'localhost'
PORT = os.getenv('MS_PORT', None) or '1433'
DATABASE = os.getenv('MS_DATABASE', None) or 'master'
DBTYPE = 'MSSQL'
APPNAME = 'test_connection'

TEST_OUTPUT_DIR = "_testoutput"
curr_file_path = os.path.join(os.path.dirname(__file__))
if not os.path.exists(os.path.join(curr_file_path, TEST_OUTPUT_DIR)):
    os.makedirs(os.path.join(curr_file_path, TEST_OUTPUT_DIR))

TEST_SCHEMA = 'dbo'
TEST_TABLE_NAME = 'test'
TEST_TABLE = '{}.test'.format(TEST_SCHEMA)
TEST_CSV_FILE = os.path.abspath(os.path.join(
    os.path.dirname(__file__), 'sample_data/unittest.csv'))
RDBMS = [mssql]
PARAMS = [{'port': PORT}
          ]
@unittest.skip
@unittest.skipIf(not os.path.exists("/workspace"), "Travis don't support MS SQLserver")
class TestMssql(unittest.TestCase):

    def populate_test_table(self, DB, fqn_table_name=TEST_TABLE):
        import pandas as pd
        import os

        dataframe = pd.read_csv(TEST_CSV_FILE)

        engine = DB.connect_SqlAlchemy()
        table_split = fqn_table_name.split('.')

        schema = None
        table = fqn_table_name
        if len(table_split) > 1:
            table = table_split[-1]
            schema = table_split[0]

        dataframe.to_sql(name=table, con=engine, index=False,
                         if_exists='replace', schema=schema)

        print("Loaded Test Data", TEST_TABLE, schema)
        print(DB.query("select * from {}".format(fqn_table_name)))

    def test_mssql(self):
        import pandas

        from shutil import copyfile
        src = os.path.join(curr_file_path, 'sample_data/AgeRange.mdb')
        dst = os.path.join(curr_file_path, TEST_OUTPUT_DIR, 'msaccess.mdb')
        copyfile(src, dst)

        x = mssql.DB(port=PORT, userid=USERID, host=HOST, dbname=DATABASE,loglevel=logging.level)
        #x = mssql.DB(port=PORT,userid=USERID,host=HOST,pwd=PASSWORD,dbname=DATABASE)
        assert isinstance(x, mssql.DB)
        # We don't want to put data into MsAccess we want to get away from access
        self.populate_test_table(DB=x, fqn_table_name=TEST_TABLE)

        z = x.get_all_tables()

        print(z)
        y = x.get_table_columns('{}.{}'.format(TEST_SCHEMA, TEST_TABLE_NAME))
        # make sure we log error
        z = x.connect_SqlAlchemy()

        print(y)
        file = os.path.join(curr_file_path, TEST_OUTPUT_DIR, 'test_mssql.csv')
        x.query_to_file(
            file, 'select * from {}'.format(TEST_TABLE_NAME), header=y, file_format='CSV')
        print(pandas.read_csv(file))

        file = os.path.join(curr_file_path, TEST_OUTPUT_DIR, 'test.parquet')
        x.query_to_file(file, 'select * from test', file_format='PARQUET')
        print(pandas.read_parquet(file, engine='pyarrow'))

        file = os.path.join(curr_file_path, TEST_OUTPUT_DIR, 'test.hdf5')
        x.query_to_file(file, 'select * from {}'.format(TEST_TABLE_NAME),
                        file_format='HDF5', hdf5_key='table')
        print(pandas.read_hdf(file, 'table'))


if __name__ == '__main__':
    unittest.main()
