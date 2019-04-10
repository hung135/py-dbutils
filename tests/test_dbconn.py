import unittest
from py_dbutils.dbconn import Connection

dbschema = 'postgres'
commit = False
password = 'docker'
userid = 'postgres'
host = 'localhost'
port = '5432'
database = 'postgres'
dbtype = 'POSTGRES'
appname = 'test_connection'



test_schema='test'
test_table='{}.test'.format(test_schema)
class TestConnection(unittest.TestCase):


    def test___del__(self):
        print("Don't know how to test this atm so passing it")
        assert True
    def test___init__(self):
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
            connection.execute("create schema if not exists {};".format(test_schema))
            connection.execute('create table if not exists {} as select 1 as col1;'.format(test_table))
        # self.assertEqual(expected, connection.__del__())
            assert True #
        except Exception as e:
            print(e)
            assert False

    def test_check_table_exists(self):
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
            self.assertEqual(True, connection.check_table_exists(test_table))
            assert True
        except Exception as e:
            print(e)
            assert False

    def test_commit(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.commit())
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
            self.assertEqual(None, connection.commit())
            assert True
        except Exception as e:
            print(e)
            assert False

    def test_connect_sqlalchemy(self):
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
            self.assertEqual("<class 'tuple'>", str(type(connection.connect_sqlalchemy())))
            assert True
        except Exception as e:
            print(e)
            assert False

    def test_copy_from_file(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.copy_from_file(dataframe, table_name_fqn, encoding))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_copy_to_csv(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.copy_to_csv(sqlstring, full_file_path, delimiter))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
            self.assertIsNotNone(connection.copy_to_csv('select * from {}'.format(test_table),'/Users/hnguyen/tmp/py-dbutils/test.csv',','))
            assert True
        except Exception as e:
            print(e)
            assert False

    def test_create_table(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.create_table(sqlstring))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_create_table_from_dataframe(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.create_table_from_dataframe(dataframe, table_name_fqn))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_drop_schema(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.drop_schema(schema))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_drop_table(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.drop_table(schema, table_name))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_dump_tables_csv(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.dump_tables_csv(table_list, folder))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_execute(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.execute(sqlstring, debug))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_execute_permit_execption(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.execute_permit_execption(sqlstring, debug))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_get_a_value(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.get_a_value(sql))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_get_all_columns_schema(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.get_all_columns_schema(dbschema, table_name))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_get_columns(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.get_columns(table_name, table_schema))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_get_conn_url(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.get_conn_url())
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_get_create_table(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.get_create_table(table_name))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_get_create_table_cli(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.get_create_table_cli(table_name, target_name, gen_pk, gen_index, gen_fk))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_get_create_table_sqlalchemy(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.get_create_table_sqlalchemy(table_name, trg_db))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_get_create_table_via_dump(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.get_create_table_via_dump(table_name, target_name, gen_pk, gen_index, gen_fk))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_get_db_size(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.get_db_size())
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_get_pandas_frame(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.get_pandas_frame(table_name, rows))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_get_primary_keys(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.get_primary_keys(table_name))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_get_schema_col_stats(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.get_schema_col_stats(schema))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_get_schema_index(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.get_schema_index(schema))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_get_table_column_types(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.get_table_column_types(table_name, trg_schema))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_get_table_columns(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.get_table_columns(table_name, trg_schema))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_get_table_list(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.get_table_list(dbschema))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_get_table_list_via_query(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.get_table_list_via_query(dbschema))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_get_table_row_count_fast(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.get_table_row_count_fast(table_name, schema))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_get_tables(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.get_tables(schema))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_get_tables_row_count(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.get_tables_row_count(schema))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_get_uncommon_tables(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.get_uncommon_tables(common_roles))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_get_view_list_via_query(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.get_view_list_via_query(dbschema))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_has_record(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.has_record(sqlstring))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_import_bulk_dataframe(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.import_bulk_dataframe(dataframe, table_name_fqn, file_delimiter, header, encoding, in_memory))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_import_file_client_side(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.import_file_client_side(full_file_path, table_name_fqn, file_delimiter, header, encoding))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_import_pyscopg2_copy(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.import_pyscopg2_copy(full_file_path, table_name_fqn, file_delimiter))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_insert_table(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.insert_table(table_name, column_list, values, onconflict))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_pandas_dump_table_csv(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.pandas_dump_table_csv(table_list, folder, chunksize))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_print_create_table(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.print_create_table(folder))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_print_drop_tables(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.print_drop_tables())
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_print_table_info(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.print_table_info(table_name, dbschema))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_print_tables(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.print_tables(table_list))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_put_pandas_frame(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.put_pandas_frame(table_name, df))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_query(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.query(sqlstring))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_rollback(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.rollback())
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_schema_exists(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.schema_exists(schema_name))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_set_table_owner(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.set_table_owner(table_name_fqn, role))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_table_exists(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.table_exists(table_name))
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.__del__())
            assert False # TODO: implement your test here
        except Exception as e:
            print(e)
            assert False

    def test_truncate_table(self):
        try:
            connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.truncate_table(table_name))
        except:
            assert False # TODO: implement your test here

    def test_update(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.update(sqlstring))
        assert False # TODO: implement your test here

    def test_vacuum(self):
        # connection = Connection(dbschema, commit, password, userid, host, port, database, dbtype, appname)
        # self.assertEqual(expected, connection.vacuum(dbschema, table_name))
        assert False # TODO: implement your test here

if __name__ == '__main__':
    unittest.main()
