import sqlalchemy
import sys
import os
import logging as lg
import datetime
from abc import ABCMeta, abstractmethod  # define interfaces

lg.basicConfig()
logging = lg.getLogger(__name__)



class DB(object):
    """Takes a query string and runs it to see if it returns any rows

            Args:
              table_name (str): String

            Returns:
              boolean: True/False
            """
    conn = None
    cursor = None
    autocommit = True

    def query(self, sql):
        """This will execute a query and fetches all the results and closes the curor.
            this is not meant to be a data store but a utility to pull bits of data from the database

        Args:(self, sql):
          sql (str): Sql compliant sytanx for the specific database type

        Returns:
          Results: Results
          List of Tuples: Column Name, Data Type
        """
        meta = None
        self.create_cur()
        """ runs query or procedure that returns record set
        """
        logging.debug('Running Query: {}\n\t{}'.format(
            datetime.datetime.now().time(), sql))
        if sql.lower().startswith('select') or sql.lower().startswith('call'):

            self.cursor.execute(sql)
            rows = self.cursor.fetchall()
            meta = self.cursor.description
            self.cursor.close()
            self.cursor = None

        else:
            raise Exception('Only Selects allowed')
        logging.debug('Query Completed: {}'.format(
            datetime.datetime.now().time()))

        return rows if isinstance(rows, list) else list(rows), meta

    def get_table_columns(self, table_name):
        """This method will select 1 record from the table and return the column names

                Args:(table_name (fully qualified):

                Returns:
                  List: List of Strings
                """

        sql = """select * from {} limit 1""".format(table_name)

        rows, meta = self.query(sql)
        # will fail if we get no rows
        return [str(r[0]) for r in meta]

    def create_cur(self):
        """This method will get called to create a cusor if one does not exist for the instance

        Args:(self):

        Returns:
          None: None
        """
        if self.cursor is None:
            self.cursor = self.conn.cursor()

    def execute_script_file_obj(self, file_object):
        """Take a sql file object specific to DB instance type and executes

        Args:
          file object: String
          commit(Boolean): True/False
          catch_exception(Boolean): True/False

        Returns:
          None: None
        """
        self.create_cur()

        self.cursor.execute(file_object.read())
        if commit or self.autocommit:
            self.commit()

    def execute_script_file(self, file_path):
        """Take a sql file specific to DB instance type and executes

        Args:
          file path: String
          commit(Boolean): True/False
          catch_exception(Boolean): True/False

        Returns:
          None: None
        """
        self.create_cur()

        with open(file_path, "r") as f:
            self.cursor.execute(f.read())
        if commit or self.autocommit:
            self.commit()

    def execute(self, sql, commit=None, catch_exception=True):
        """Take a sql string specific to DB instance type and executes

        Args:
          sql (str): String
          commit(Boolean): True/False
          catch_exception(Boolean): True/False

        Returns:
          None: None
        """
        self.create_cur()
        logging.debug(
            "Debug DB Execute: {}: \n\t{} ".format(self.__str__(), sql))
        rowcount = 0
        this_sql = str(sql).strip()

        
        try:
            self.cursor.execute(this_sql)

        except Exception as e:
            
            logging.error(  "SQL error:\n{}".format(e))
            if catch_exception:
                logging.warning(
                    "SQL error Occurred But Continuing:\n{}")
            else:
                raise Exception('Raising ERROR for:', sql)
         
        rowcount = self.cursor.rowcount
        if commit or self.autocommit:
            self.commit()

        logging.debug("DB SQL Execute Completed: {}".format(self.__str__()))

        return rowcount

    def bulk_load(self):
        pass

    def commit(self):
        """Commits anything from the current open connection and closes the cursor

        Args:(self):

        Returns:
          None: None
        """
        """ Default to commit after every transaction
                Will check instance variable to decide if a commit is needed
        """
        try:
            self.cursor.execute("COMMIT")
            self.cursor.close()
            self.cursor = None
        except AttributeError:
            logging.error("No Open Cursor to do Commit")
        except Exception as e:
            logging.error(e)

    def rollback(self):
        """Rolls back anything the instance has run and closes the cursor

        Args:(self):
          table_name (str): String

        Returns:
          None: None
        """
        self.cursor.execute("ROLLBACK")
        self.cursor.close()
        self.cursor = None

    def query_to_parquet(self, file_path, sql):
        """
        Uses Pandas and SqlAchemy to dump data to a parquet file
        :param file_path:
        :param sql:
        :return:
        """
        import pyarrow.parquet as pq
        import pyarrow as pa
        import pandas
        df = pandas.read_sql(sql, self.connect_SqlAlchemy())

        table = pa.Table.from_pandas(df)
        pq.write_table(table, os.path.abspath(file_path))

    def query_to_hdf5(self, file_path, sql, key='table'):
        """Uses Pandas and SqlAchemy to dump data to a CSV file
        :param file_path:
        :param sql:
        :param key: They Grouping in which the data falls under inside HDF5
        :return:
        """
        import pandas

        df = pandas.read_sql(sql, self.connect_SqlAlchemy())
        df.to_hdf(path_or_buf=os.path.abspath(file_path), key=key, mode='w')

    def query_to_csv(self, file_path, sql, include_header=True):
        """Uses Pandas and SqlAchemy to dump data to a CSV file
        :param file_path:
        :param sql:
        :param include_header:
        :return:
        """
        import pandas
        df = pandas.read_sql(sql, self.connect_SqlAlchemy())
        df.to_csv(path_or_buf=os.path.abspath(
            file_path), header=include_header)

    def query_to_file(self, file_path, sql, file_format='CSV', header=True, hdf5_key=None):
        """Generic Query to file will work for any database we can make a connection to w/out the need for SQLalchemy
        :param file_path:
        :param sql:
        :param file_format:
        :param header: Boolean (Print Header in File: True/False) or your list of header names
        :param hdf5_key: Only needed when format is HDF5
        :return:
        """
        import pandas
        rows, meta = self.query(sql)

        column_list = []
        for a in meta:
            column_list.append(a[0])
        csv_header = None
        if header == True:

            csv_header = column_list
        elif header is None or header == False:
            csv_header = False
        else:
            csv_header = header
        df = pandas.DataFrame(data=rows, columns=column_list)

        if file_format == 'CSV':
            df.to_csv(path_or_buf=os.path.abspath(file_path),
                      header=csv_header, index=False)
        if file_format == 'PARQUET':
            # keeps from having to import this dependency if we never use this file format
            import pyarrow.parquet as pq
            import pyarrow as pa
            table = pa.Table.from_pandas(df)
            pq.write_table(table, os.path.abspath(file_path))
        if file_format == 'HDF5':
            df.to_hdf(path_or_buf=os.path.abspath(
                file_path), key=hdf5_key, mode='w')

    def schema_exists(self, schema_name):
        """Takes a query string and runs it to see if it returns any rows

        Args:
          table_name (str): String

        Returns:
          boolean: True/False
        """
        self.create_cur()
        v_found = False

        v_found = self.has_record(
            """select 1 from information_schema.schemata where schema_name='{0}' limit 1""".format(schema_name))

        return v_found

    def table_exists(self, fqn_table_name):
        """Takes a query string and runs it to see if it returns any rows

        Args:
          table_name (str): String

        Returns:
          boolean: True/False
        """
        self.create_cur()

        v_table_exists = False
        v_schema = fqn_table_name.split('.')[0]
        v_table_name = fqn_table_name.split('.')[1]
        v_table_exists = self.has_record(
            """select 1 from information_schema.tables where table_schema='{0}' and table_name='{1}'""".format(v_schema,
                                                                                                               v_table_name))

        return v_table_exists

    def get_a_row(self, sql):
        """Returns 1 row as tuple:
            use var,=row for 1 element tuple
        Args:(self, sql):
          sql (str): String

        Returns:
          row: tuple
        """
        self.create_cur()
        self.cursor.execute(sql)
        row = self.cursor.fetchone()
        self.cursor.close()
        self.cursor = None

        return row

    def has_record(self, sql):
        """Takes a query string and runs it to see if it returns any rows

        Args:
          sql (str): String

        Returns:
          boolean: True/False
        """
        self.create_cur()

        try:
            row = self.get_a_row(sql)
            if row is None:
                return False
            else:
                return True
        except Exception as e:
            logging.error("Error in db.has_record: {}\n{}".format(sql, e))
        return False

    # ensure this method gets implement or inherited somewhere by child
    @abstractmethod
    def connect_SqlAlchemy(self): raise NotImplementedError

    def create_table_from_dataframe(self, dataframe, table_name_fqn, default_owner=None):
        """Describe Method:

        Args:(self, dataframe, table_name_fqn):
          table_name (str): String

        Returns:
          None: None
        """
        if '.' in table_name_fqn:
            if not self.table_exists(table_name_fqn):
                self.create_cur()
                df = dataframe.head()
                schema = table_name_fqn.split('.')[0]
                table_name = table_name_fqn.split('.')[1]

                engine = self.connect_SqlAlchemy()  # dependent on the child

                df.to_sql(table_name, engine, schema=schema,
                          if_exists='append', index=False, chunksize=1000)

                self.execute('truncate table {}'.format(table_name_fqn))
                return True
            else:
                logging.info("Table exists: {}".format(table_name_fqn))
                return False
        else:
            logging.error('Please provide fully qualified table name')
            return False

    def get_all_tables(self):
        """Returns list of all tables visible to this connection

        Args:(self, dbschema):
          table_name (str): String

        Returns:
          None: None
        """
        sql = """SELECT concat(table_schema,'.',table_name) as table_name FROM information_schema.tables a
            WHERE table_type='BASE TABLE'"""
        result_set, meta = self.query(sql)
        return [r[0] for r in result_set]


class ConnRDBMS(object):
    userid = None
    pwd = None
    host = None
    port = None
    dbname = None
    autocommit = True
    sql_alchemy_uri = None
    connected_uri = None
    conn = None
    appname = __file__

    def __init__(self, autocommit=None, pwd=None, userid=None, host=None, dbname=None, schema=None, loglevel=None):
        
        logging.level=loglevel
        self.str = f'DB: {self.host}:{self.port}:{self.dbname}:{self.userid}:autocommit={self.autocommit}' 
        
        try:

            self.cursor = None
            self.autocommit = autocommit or True
            logging.debug(f'INIT DB: {self.host}:{self.port}:{self.dbname}:{self.userid}:autocommit={self.autocommit}' )
            logging.debug(f'Connect Info: {self.host}:{self.dbname}:{self.userid})' )
        except Exception:
            logging.exception( "Can not U se this Class directly: You must instantiate a child")
            sys.exit(1)

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        str = None
        try:
            str = self.str
        except Exception:
            pass
        return str

    def __del__(self):
        logging.debug(f"Destroying: {self.__str__()}")

    def authenticate(self):
        pass

    def close(self):
        # self.cursor.close()
        self.conn.close()

    def connect_SqlAlchemy(self):

        if self.sql_alchemy_uri is None:
            logging.error("sqlAlchemy not supported for this DB")
            sys.exit(1)
        else:
            try:
                self.connected_uri = self.sql_alchemy_uri.format(
                    userid=self.userid,
                    pwd=self.pwd,
                    host=self.host,
                    port=self.port,
                    db=self.dbname,
                    appname=self.appname)
                return sqlalchemy.create_engine(self.sql_alchemy_uri.format(
                    userid=self.userid,
                    pwd=self.pwd,
                    host=self.host,
                    port=self.port,
                    db=self.dbname,
                    appname=self.appname
                ))
            except Exception as e:
                logging.error( f"Could not Connect to sqlAlchemy, Check Uri Syntax: {e}" )
                sys.exit(1)


class ConnREST(object):
    def __init__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __repr__(self):
        pass

    def __str__(self):
        pass

    def __del__(self):
        pass

    def authenticate(self):
        pass

    def close(self):
        pass
