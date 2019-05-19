import unittest
import os
import sys

# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from py_dbutils.rdbms import postgres
import inspect
import os
import pprint
import pandas as pd

DBSCHEMA = 'test'
COMMIT = True
PASSWORD = os.getenv('PGPASSWORD', None) or 'docker'
USERID = os.getenv('PGUSER', None) or 'docker'
HOST = os.getenv('PGHOST', None) or 'localhost'
PORT = os.getenv('PGPORT', None) or '5432'
DATABASE = os.getenv('PGDATABASE', None) or 'postgres'
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
RDBMS = [postgres]
PARAMS = [{'port': PORT}
          ]


class TestPostgres(unittest.TestCase):

    def populate_test_table(self, DB, fqn_table_name=TEST_TABLE):
        import os
        DB.execute("Create schema {}".format(DBSCHEMA))
        DB.commit()
        dataframe = pd.read_csv(TEST_CSV_FILE)

        engine = DB.connect_SqlAlchemy()
        table_split = fqn_table_name.split('.')

        schema = DBSCHEMA
        table = fqn_table_name
        if len(table_split) > 1:
            table = table_split[-1]
            schema = table_split[0]
        
        dataframe.to_sql(name=table, con=engine, index=False, if_exists='replace', schema=schema)

        print("Loaded Test Data")
        print(DB.query("select * from {}".format(TEST_TABLE)))

    def test_postgres(self):
        from shutil import copyfile
        src = os.path.join(curr_file_path, 'sample_data/AgeRange.mdb')
        dst = os.path.join(curr_file_path, TEST_OUTPUT_DIR, 'msaccess.mdb')
        copyfile(src, dst)

        print("-------------------------",USERID)
        x = postgres.DB(port=PORT,userid=USERID,host=HOST,pwd=PASSWORD,dbname=DATABASE)
        
        assert isinstance(x, postgres.DB)
        # We don't want to put data into MsAccess we want to get away from access
        self.populate_test_table(DB=x, fqn_table_name=TEST_TABLE)

        z = x.get_all_tables()

        print(z)
        y = x.get_table_columns(TEST_TABLE)
        # make sure we log error
        z = x.connect_SqlAlchemy()
        file = os.path.join(curr_file_path, TEST_OUTPUT_DIR, 'test_postres.csv')
        x.query_to_file(file, 'select * from {}'.format(TEST_TABLE), header=y, file_format='CSV')
        print(pd.read_csv(file))

        file = os.path.join(curr_file_path, TEST_OUTPUT_DIR, 'test.parquet')
        x.query_to_file(file, 'select * from {}'.format(TEST_TABLE), file_format='PARQUET')
        print(pd.read_parquet(file, engine='pyarrow'))

        file = os.path.join(curr_file_path, TEST_OUTPUT_DIR, 'test.hdf5')
        x.query_to_file(file, 'select * from {}'.format(TEST_TABLE), file_format='HDF5', hdf5_key='table')
        print(pd.read_hdf(file, 'table'))

    def test_bulk_load_dataframe(self):
        db = postgres.DB(port=PORT,userid=USERID,host=HOST,pwd=PASSWORD,dbname=DATABASE)
        df = pd.read_csv(TEST_CSV_FILE)
        print(db.get_table_columns(TEST_TABLE))
        #db.execute('truncate table test.test')
        db.bulk_load_dataframe(dataframe=df, table_name_fqn=TEST_TABLE, encoding='utf8', workingpath='MEMORY')
        db.execute('truncate table {}'.format(TEST_TABLE))
        db.bulk_load_dataframe(dataframe=df, table_name_fqn=TEST_TABLE, encoding='utf8',
                               workingpath=os.path.join(curr_file_path, TEST_OUTPUT_DIR))
        db.commit()
       
        print(db.query('select * from {}'.format(TEST_TABLE)))


if __name__ == '__main__':
    unittest.main()
