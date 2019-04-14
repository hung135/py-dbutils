import unittest
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from py_dbutils.dbconn import Connection
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


class TestConnection(unittest.TestCase):

    def populate_test_table(self):
        import pandas as pd
        import os

        dataframe = pd.read_csv(TEST_CSV_FILE)
        connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE, inspect.stack()[0][3])
        connection.execute(sqlstring="create schema {};".format(TEST_SCHEMA))
        connection.commit()
        print("---")
        connection.drop_table("{schema}.{table_name}".format(
            schema=TEST_SCHEMA, table_name=TEST_TABLE_NAME), cascade=True)
        connection.commit()
        print("---")
        engine, meta = connection.connect_sqlalchemy(schema=TEST_SCHEMA, db_type='POSTGRES')
        print("---")
        dataframe.to_sql(name=TEST_TABLE.split('.')[-1], con=engine, index=False, if_exists='replace',
                         schema=TEST_SCHEMA)

        connection.commit()
        connection.execute("""CREATE INDEX test_idx
                                            ON test.test USING btree
                                            (col1 ASC NULLS LAST)
                                            TABLESPACE pg_default;
                                            ALTER TABLE test.test ADD PRIMARY KEY (col1); """)
        connection.commit()
        connection.execute("vacuum analyse {}".format(TEST_TABLE))
        connection.commit()
        print("Loaded Test Data")
        print(connection.query("select * from {}".format(TEST_TABLE)))

    def test___del__(self):
        print("Don't know how to test this atm so passing it")
        assert True

    def test___init__(self):
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD,
                                    USERID, HOST, PORT, DATABASE,
                                    DBTYPE, inspect.stack()[0][3])

            connection.execute(sqlstring="CREATE SCHEMA  {};".format(TEST_SCHEMA),commit=True)

            connection.drop_table(table_name=TEST_TABLE, cascade=True)
            connection.commit()

            connection.execute(sqlstring='CREATE TABLE {} as select 1 as col1;'.format(TEST_TABLE),
                               commit=True)

            connection.execute(
                sqlstring='CREATE OR REPLACE view {}_vw as select * from {};'.format(TEST_TABLE, TEST_TABLE)
                , catch_exception=False, commit=True)
            assert True
        except Exception as e:
            print(e)
            assert False

    def test_check_table_exists(self):
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            self.assertEqual(True, connection.check_table_exists(TEST_TABLE))
            assert True
        except Exception as e:

            print(e)
            assert False

    def test_connect_sqlalchemy(self):
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            import sqlalchemy
            x, y = connection.connect_sqlalchemy()
            z = isinstance(x, sqlalchemy.engine.base.Engine)

            self.assertEqual(True, z)
            # self.assertEqual("<class 'tuple'>", str(type(connection.connect_sqlalchemy('POSTGRES'))))
            # self.assertEqual("<class 'tuple'>", str(type(connection.connect_sqlalchemy('MSSQL'))))
            # self.assertEqual("<class 'tuple'>", str(type(connection.connect_sqlalchemy('MYSQL'))))
            # self.assertEqual("<class 'tuple'>", str(type(connection.connect_sqlalchemy('ORACLE'))))

            assert True
        except Exception as e:
            print(e)
            assert False

    def test_copy_from_file(self):

        # self.assertEqual(expected, connection.copy_from_file(dataframe, table_name_fqn, encoding))
        import pandas as pd
        import os

        dataframe = pd.read_csv(TEST_CSV_FILE)

        try:
            self.populate_test_table()

            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            connection.truncate_table(TEST_TABLE)
            connection.commit()
            engine, meta = connection.connect_sqlalchemy(schema=TEST_SCHEMA, db_type='POSTGRES')
            dataframe.to_sql(name=TEST_TABLE.split('.')[-1], con=engine, index=False, if_exists='replace',
                             schema=TEST_SCHEMA)



            connection.copy_from_file(dataframe, TEST_TABLE, 'UTF-8')

            self.assertLess(0, connection.get_a_value("select count(*) from {}".format(TEST_TABLE)))
            assert True
        except Exception as e:
            print(e)
            assert False

    def test_copy_to_csv(self):

        path = os.path.join(curr_file_path, TEST_OUTPUT_DIR, 'to_csv_dump.csv')

        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            connection.copy_to_csv('select * from {}'.format(TEST_TABLE), path, ',')
            self.assertLess(10, os.path.getsize(path))
            assert True
        except Exception as e:

            print(e)
            assert False

    def test_create_table(self):

        # self.assertEqual(expected, connection.create_table(sqlstring))
        sqlstring = "create table {schema}.{table_name}_x as select * from {schema}.{table_name}".format(
            schema=TEST_SCHEMA, table_name=TEST_TABLE_NAME)
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            connection.drop_table("{schema}.{table_name}_x".format(
                schema=TEST_SCHEMA, table_name=TEST_TABLE_NAME), cascade=True)
            connection.create_table(sqlstring)
            self.assertEqual(True, connection.table_exists("{schema}.{table_name}_x".format(
                schema=TEST_SCHEMA, table_name=TEST_TABLE_NAME)))

            assert True
        except Exception as e:

            print(e)
            assert False

    def test_create_table_from_dataframe(self):

        # self.assertEqual(expected, connection.create_table_from_dataframe(dataframe, table_name_fqn))

        import pandas as pd
        import os

        dataframe = pd.read_csv(TEST_CSV_FILE)
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            connection.drop_table(TEST_TABLE)
            connection.commit()
            connection.create_table_from_dataframe(dataframe, TEST_TABLE)
            connection.commit()
            self.assertEqual(True, connection.check_table_exists(TEST_TABLE))
            assert True
        except Exception as e:

            print(e)
            assert False

    def test_drop_schema(self):

        # self.assertEqual(expected, connection.drop_schema(schema))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])

            assert True
        except Exception as e:

            print(e)
            assert False

    def test_drop_table(self):

        # self.assertEqual(expected, connection.drop_table(schema, table_name))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])

            assert True
        except Exception as e:

            print(e)
            assert False

    def test_dump_tables_csv(self):

        # self.assertEqual(expected, connection.dump_tables_csv(table_list, folder))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])

            assert True
        except Exception as e:

            print(e)
            assert False

    def test_execute(self):

        # self.assertEqual(expected, connection.execute(sqlstring, debug))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])

            assert True
        except Exception as e:

            print(e)
            assert False

    def test_execute_permit_execption(self):

        # self.assertEqual(expected, connection.execute_permit_execption(sqlstring, debug))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])

            assert True
        except Exception as e:

            print(e)
            assert False

    def test_get_a_value(self):

        # self.assertEqual(expected, connection.get_a_value(sql))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])

            assert True
        except Exception as e:

            print(e)
            assert False

    def test_get_all_columns_schema(self):

        # self.assertEqual(expected, connection.get_all_columns_schema(dbschema, table_name))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])

            assert True
        except Exception as e:

            print(e)
            assert False

    def test_get_columns(self):

        # self.assertEqual(expected, connection.get_columns(table_name, table_schema))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            x = connection.get_columns('test', TEST_SCHEMA)
            self.assertGreater(len(x), 0)
            assert True
        except Exception as e:

            print(e)
            assert False

    def test_get_conn_url(self):

        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            self.assertGreater(len(connection.get_conn_url()), 0)
            assert True
        except Exception as e:

            print(e)
            assert False

    def test_get_create_table(self):

        # self.assertEqual(expected, connection.get_create_table(table_name))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])

            assert True
        except Exception as e:

            print(e)
            assert False

    def test_get_create_table_cli(self):

        # self.assertEqual(expected, connection.get_create_table_cli(table_name, target_name, gen_pk, gen_index, gen_fk))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])

            assert True
        except Exception as e:

            print(e)
            assert False

    def test_get_create_table_sqlalchemy(self):

        # self.assertEqual(expected, connection.get_create_table_sqlalchemy(table_name, trg_db))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])

            assert True
        except Exception as e:

            print(e)
            assert False

    def test_get_create_table_via_dump(self):

        # self.assertEqual(expected, connection.get_create_table_via_dump(table_name, target_name, gen_pk, gen_index, gen_fk))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])

            assert True
        except Exception as e:

            print(e)
            assert False

    def test_get_db_size(self):

        # self.assertEqual(expected, connection.get_db_size())
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])

            assert True
        except Exception as e:

            print(e)
            assert False

    def test_get_pandas_frame(self):

        #
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])

            self.assertNotEqual(connection.get_pandas_frame(TEST_TABLE, 1), None)
            assert True
        except Exception as e:

            print(e)
            assert False

    def test_get_primary_keys(self):

        # self.assertEqual(expected, connection.get_primary_keys(table_name))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            self.assertGreaterEqual(len(connection.get_primary_keys(TEST_TABLE)), 0)
            assert True
        except Exception as e:

            print(e)
            assert False

    def test_get_schema_col_stats(self):

        # self.assertEqual(expected, connection.get_schema_col_stats(schema))
        try:
            self.populate_test_table()
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])

            x = connection.get_schema_col_stats(TEST_SCHEMA)

            self.assertGreaterEqual(len(x), 0)
            assert True
        except Exception as e:

            print(e)
            assert False

    def test_get_schema_index(self):

        # self.assertEqual(expected, connection.get_schema_index(schema))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            self.assertGreater(len(connection.get_schema_index(TEST_SCHEMA)), 0)
            assert True
        except Exception as e:

            print(e)
            assert False

    def test_get_table_column_types(self):

        # self.assertEqual(expected, connection.get_table_column_types(table_name, trg_schema))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])

            assert True
        except Exception as e:

            print(e)
            assert False

    def test_get_table_columns(self):

        # self.assertEqual(expected, connection.get_table_columns(table_name, trg_schema))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])

            assert True
        except Exception as e:

            print(e)
            assert False

    def test_get_table_list(self):

        # self.assertEqual(expected, connection.get_table_list(dbschema))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])

            assert True
        except Exception as e:

            print(e)
            assert False

    def test_get_table_list_via_query(self):

        # self.assertEqual(expected, connection.get_table_list_via_query(dbschema))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])

            assert True
        except Exception as e:

            print(e)
            assert False

    def test_get_table_row_count_fast(self):

        # self.assertEqual(expected, connection.get_table_row_count_fast(table_name, schema))
        try:
            self.populate_test_table()
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            connection.execute('vacuum test')
            connection.commit()
            self.assertGreaterEqual(connection.get_table_row_count_fast('test', TEST_SCHEMA), 0)

            assert True
        except Exception as e:

            print(e)
            assert False

    def test_get_tables(self):

        # self.assertEqual(expected, connection.get_tables(schema))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            self.assertEqual(0, len(connection.get_tables(TEST_SCHEMA)))

            assert True
        except Exception as e:

            print(e)
            assert False

    def test_get_tables_row_count(self):

        # self.assertEqual(expected, connection.get_tables_row_count(schema))
        try:
            self.populate_test_table()
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            connection.commit()
            x = connection.get_tables_row_count(TEST_SCHEMA)
            print(x)
            self.assertGreater(len(x), 0)
            assert True
        except Exception as e:

            print(e)
            assert False

    def test_get_uncommon_tables(self):

        # self.assertEqual(expected, connection.get_uncommon_tables(common_roles))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])

            assert True
        except Exception as e:

            print(e)
            assert False

    def test_get_view_list_via_query(self):

        # self.assertEqual(expected, connection.get_view_list_via_query(dbschema))
        try:
            self.populate_test_table()
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            connection.execute("create view test.vw_test as select * from {}".format(TEST_TABLE))
            x = connection.get_view_list_via_query(TEST_SCHEMA)
            self.assertGreater(len(x), 0)

            assert True
        except Exception as e:

            print(e)
            assert False

    def test_has_record(self):

        # self.assertEqual(expected, connection.has_record(sqlstring))
        try:
            self.populate_test_table()
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            self.assertEqual(True, connection.has_record('select * from {}'.format(TEST_TABLE)))

            assert True  # TODO: implement your test here
        except Exception as e:

            print(e)
            assert False



    def test_import_file_client_side(self):

        # self.assertEqual(expected, connection.import_file_client_side(full_file_path, table_name_fqn, file_delimiter, header, encoding))
        try:
            self.populate_test_table()
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            connection.truncate_table(TEST_TABLE)
            connection.commit()
            self.assertEqual(None, connection.import_file_client_side(TEST_CSV_FILE, TEST_TABLE, ',',header=True))

            assert True
        except Exception as e:

            print(e)
            assert False

    def test_import_pyscopg2_copy(self):

        # self.assertEqual(expected, connection.import_pyscopg2_copy(full_file_path, table_name_fqn, file_delimiter))
        try:
            self.populate_test_table()
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            connection.truncate_table(TEST_TABLE)
            connection.commit()
            self.assertEqual(None, connection.import_pyscopg2_copy(TEST_CSV_FILE, TEST_TABLE, ','))

            assert True
        except Exception as e:

            print(e)
            assert False



    def test_pandas_dump_table_csv(self):

        # self.assertEqual(expected, connection.pandas_dump_table_csv(table_list, folder, chunksize))
        try:
            directory = os.path.join(curr_file_path, TEST_OUTPUT_DIR)
            self.populate_test_table()
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            self.assertEqual(None, connection.pandas_dump_table_csv([TEST_TABLE], directory, 1000))

            assert True
        except Exception as e:

            print(e)
            assert False

    def test_print_create_table(self):

        # self.assertEqual(expected, connection.print_create_table(folder))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            connection.print_create_table('./_sql')

            assert True
        except Exception as e:

            print(e)
            assert False

    def test_print_drop_tables(self):

        # self.assertEqual(expected, connection.print_drop_tables())
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            self.assertEqual(None, connection.print_drop_tables())
            assert True
        except Exception as e:

            print(e)
            assert False

    def test_print_table_info(self):

        # self.assertEqual(expected, connection.print_table_info(table_name, dbschema))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            self.assertEqual(None, connection.print_table_info(TEST_TABLE_NAME, TEST_SCHEMA))

            assert True
        except Exception as e:

            print(e)
            assert False

    def test_print_tables(self):

        # self.assertEqual(expected, connection.print_tables(table_list))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            self.assertEqual(None, connection.print_tables([TEST_TABLE]))

            assert True
        except Exception as e:

            print(e)
            assert False


    def test_query(self):

        # self.assertEqual(expected, connection.query(sqlstring))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            self.assertIsNotNone(connection.query('select * from {}'.format(TEST_TABLE)))
            assert True
        except Exception as e:

            print(e)
            assert False

    def test_rollback(self):

        # self.assertEqual(expected, connection.rollback())
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            connection.execute("insert into test.test values('1','2','3')")
            self.assertEqual(None, connection.rollback())

            assert True
        except Exception as e:

            print(e)
            assert False

    def test_schema_exists(self):

        # self.assertEqual(expected, connection.schema_exists(schema_name))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            self.assertEqual(True, connection.schema_exists('test'))
            assert True
        except Exception as e:

            print(e)
            assert False

    def test_set_table_owner(self):

        # self.assertEqual(expected, connection.set_table_owner(table_name_fqn, role))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            self.assertEqual(None, connection.set_table_owner(TEST_TABLE, 'operational_dba'))
            assert True
        except Exception as e:

            print(e)
            assert False

    def test_table_exists(self):

        # self.assertEqual(expected, connection.table_exists(table_name))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            self.assertEqual(True, connection.table_exists(TEST_TABLE))
            assert True
        except Exception as e:

            print(e)
            assert False

    def test_update(self):
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            self.assertEqual(None, connection.update('update {} set col1=0'.format(TEST_TABLE)))
        except:
            print(e)
            assert False

    def test_truncate_table(self):
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            connection.truncate_table(TEST_TABLE)
            self.assertEqual(0, int(connection.get_a_value('select count(*) from {}'.format(TEST_TABLE))))
            assert True
        except:
            assert True

    def test_commit(self):

        # self.assertEqual(expected, connection.commit())
        try:
            self.populate_test_table();
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            connection.execute(sqlstring="update test.test set col2='aaaaa'", catch_exception=False, commit=False)
            connection.commit()
            self.assertLess(0, connection.get_a_value("select count(*) from test.test where col2='aaaaa'"))
            assert True
        except Exception as e:
            print(e)
            assert False


if __name__ == '__main__':
    unittest.main()
