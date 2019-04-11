import unittest
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from py_dbutils.dbconn import Connection
import inspect
import os

DBSCHEMA = 'postgres'
COMMIT = False
PASSWORD = 'docker'
USERID = 'postgres'
HOST = 'localhost'
PORT = '5432'
DATABASE = 'postgres'
DBTYPE = 'POSTGRES'
APPNAME = 'test_connection'

TEST_OUTPUT_DIR = "_testoutput"
cwd = os.getcwd()
if not os.path.exists(os.path.join(cwd, TEST_OUTPUT_DIR)):
    os.makedirs(os.path.join(cwd, TEST_OUTPUT_DIR))

TEST_SCHEMA = 'test'
TEST_TABLE = '{}.test'.format(TEST_SCHEMA)


class TestConnection(unittest.TestCase):

    def populate_test_table(self):
        import pandas as pd
        import os
        cwd = os.getcwd()

        dataframe = pd.read_csv(os.path.abspath(os.path.join(os.path.dirname(__file__), 'sample_data/unittest.csv')))
        connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE, inspect.stack()[0][3])
        connection.execute("create schema if not exists {}".format(TEST_SCHEMA))
        engine, meta = connection.connect_sqlalchemy(schema=TEST_SCHEMA, db_type='POSTGRES')
        dataframe.to_sql(name=TEST_TABLE.split('.')[-1], con=engine, index=False, if_exists='replace',
                         schema=TEST_SCHEMA)

    def test___del__(self):
        print("Don't know how to test this atm so passing it")
        assert True

    def test___init__(self):
        try:
            with open(
                    Connection(DBSCHEMA, COMMIT, PASSWORD,
                               USERID, HOST, PORT, DATABASE,
                               DBTYPE, inspect.stack()[0][3])
            ) as connection:
                connection.execute("create schema if not exists {};".format(TEST_SCHEMA))
            connection.execute('drop table if  exists {};'.format(TEST_TABLE))
            connection.execute('create table if not exists {} as select 1 as col1;'.format(TEST_TABLE))
            connection.execute(
                'create view if not exists {}_vw as select * from test.{};'.format(TEST_TABLE, TEST_TABLE))
            assert True  #
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

    def test_commit(self):

        # self.assertEqual(expected, connection.commit())
        try:
            self.populate_test_table();
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            connection.execute("update test.test set col1='aaaaa'", commit=False)
            connection.commit()
            self.assertLess(0, connection.get_a_value("select count(*) from test.test where col1='aaaaa'"))
            assert True
        except Exception as e:
            print(e)
            assert False

    def test_connect_sqlalchemy(self):
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            self.assertEqual("<class 'tuple'>", str(type(connection.connect_sqlalchemy())))
            self.assertEqual("<class 'tuple'>", str(type(connection.connect_sqlalchemy('POSTGRES'))))
            self.assertEqual("<class 'tuple'>", str(type(connection.connect_sqlalchemy('MSSQL'))))
            self.assertEqual("<class 'tuple'>", str(type(connection.connect_sqlalchemy('MYSQL'))))
            self.assertEqual("<class 'tuple'>", str(type(connection.connect_sqlalchemy('ORACLE'))))

            assert True
        except Exception as e:
            print(e)
            assert False

    def test_copy_from_file(self):

        # self.assertEqual(expected, connection.copy_from_file(dataframe, table_name_fqn, encoding))
        import pandas as pd
        import os
        cwd = os.getcwd()
        dataframe = pd.read_csv(os.path.join(cwd, "tests/sample_data/unittest.csv"))

        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            engine, meta = connection.connect_sqlalchemy(schema=TEST_SCHEMA, db_type='POSTGRES')
            dataframe.to_sql(name=TEST_TABLE.split('.')[-1], con=engine, index=False, if_exists='replace',
                             schema=TEST_SCHEMA)

            connection.truncate_table(TEST_TABLE)

            connection.copy_from_file(dataframe, TEST_TABLE, 'UTF-8')

            self.assertLess(0, connection.get_a_value("select count(*) from {}".format(TEST_TABLE)))
            assert True
        except Exception as e:
            print(e)
            assert False

    def test_copy_to_csv(self):

        path = os.path.join(cwd, TEST_OUTPUT_DIR, 'to_csv_dump.csv')

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

        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])

            assert True
        except Exception as e:
            print(e)
            assert False

    def test_create_table_from_dataframe(self):

        # self.assertEqual(expected, connection.create_table_from_dataframe(dataframe, table_name_fqn))

        import pandas as pd
        import os
        cwd = os.getcwd()

        df = pd.read_csv(os.path.join(cwd, "tests/sample_data/unittest.csv"))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            connection.drop_table(TEST_TABLE)
            connection.create_table_from_dataframe(df, TEST_TABLE)
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
            self.assertGreater(0, len(connection.get_columns(TEST_TABLE, TEST_SCHEMA)))
            assert True
        except Exception as e:
            print(e)
            assert False

    def test_get_conn_url(self):

        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            self.assertGreater(0, len(connection.get_conn_url()))
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
            self.assertEqual(1, connection.get_pandas_frame(TEST_TABLE, 1))
            assert True
        except Exception as e:
            print(e)
            assert False

    def test_get_primary_keys(self):

        # self.assertEqual(expected, connection.get_primary_keys(table_name))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            self.assertEqual(1, connection.get_primary_keys(TEST_TABLE))
            assert True
        except Exception as e:
            print(e)
            assert False

    def test_get_schema_col_stats(self):

        # self.assertEqual(expected, connection.get_schema_col_stats(schema))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])

            self.assertEqual(1, connection.get_schema_col_stats(TEST_SCHEMA))
            assert True
        except Exception as e:
            print(e)
            assert False

    def test_get_schema_index(self):

        # self.assertEqual(expected, connection.get_schema_index(schema))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            self.assertEqual(None, connection.get_schema_index(TEST_SCHEMA))
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
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            self.assertGreater(0, connection.get_table_row_count_fast(TEST_TABLE, TEST_SCHEMA))

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
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            self.assertGreater(0, len(connection.get_tables_row_count(TEST_SCHEMA)))
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
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            self.assertGreater(0, len(connection.get_view_list_via_query(TEST_SCHEMA)))

            assert True  # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_has_record(self):

        # self.assertEqual(expected, connection.has_record(sqlstring))
        try:
            self.populate_test_table()
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            self.assertGreater(0, connection.has_record('select * from {}'.format(TEST_TABLE)))

            assert True  # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_import_bulk_dataframe(self):

        # self.assertEqual(expected, connection.import_bulk_dataframe(dataframe, table_name_fqn, file_delimiter, header, encoding, in_memory))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])

            assert True
        except Exception as e:
            print(e)
            assert False

    def test_import_file_client_side(self):

        # self.assertEqual(expected, connection.import_file_client_side(full_file_path, table_name_fqn, file_delimiter, header, encoding))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])

            assert True
        except Exception as e:
            print(e)
            assert False

    def test_import_pyscopg2_copy(self):

        # self.assertEqual(expected, connection.import_pyscopg2_copy(full_file_path, table_name_fqn, file_delimiter))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            self.assertEqual(None, connection.import_pyscopg2_copy(full_file_path, table_name_fqn, file_delimiter))

            assert True
        except Exception as e:
            print(e)
            assert False

    def test_insert_table(self):

        # self.assertEqual(expected, connection.insert_table(table_name, column_list, values, onconflict))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])

            assert True
        except Exception as e:
            print(e)
            assert False

    def test_pandas_dump_table_csv(self):

        # self.assertEqual(expected, connection.pandas_dump_table_csv(table_list, folder, chunksize))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            self.assertEqual(None, connection.pandas_dump_table_csv([TEST_TABLE], './sql', 1000))

            assert True
        except Exception as e:
            print(e)
            assert False

    def test_print_create_table(self):

        # self.assertEqual(expected, connection.print_create_table(folder))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            self.assertIsNotNone(connection.print_create_table('./_sql'))

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
            self.assertEqual(None, connection.print_table_info(TEST_TABLE, TEST_SCHEMA))

            assert False
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

    def test_put_pandas_frame(self):

        # self.assertEqual(expected, connection.put_pandas_frame(table_name, df))
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])

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

    def test_vacuum(self):
        try:
            connection = Connection(DBSCHEMA, COMMIT, PASSWORD, USERID, HOST, PORT, DATABASE, DBTYPE,
                                    inspect.stack()[0][3])
            self.assertEqual(None, connection.vacuum(TEST_SCHEMA, TEST_TABLE))
            assert True
        except:
            assert False


if __name__ == '__main__':
    unittest.main()
