import sys
import os
import logging as lg
import datetime

lg.basicConfig()
logging = lg.getLogger()
logging.setLevel(lg.INFO)
import sqlalchemy


class DB(object):
    """Takes a query string and runs it to see if it returns any rows

            Args:
              table_name (str): String

            Returns:
              boolean: True/False
            """

    def __init__(self):
        print("Init SUERP DB")
        self.cursor=None


    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __repr__(self):
        pass

    def __str__(self):
        pass

    def __del__(self):
        pass

    def query(self, sql):
        """This will execute a query and fetches all the results and closes the curor.
            this is not meant to be a data store but a utility to pull bits of data from the database

        Args:(self, sql):
          sql (str): Sql compliant sytanx for the specific database type

        Returns:
          None: None
        """
        self.create_cur()
        """ runs query or procedure that returns record set
        """
        logging.debug('Running Query: {}\n\t{}'.format(datetime.datetime.now().time(), sql))
        if sql.lower().startswith('select') or sql.lower().startswith('call'):
            self.cursor.execute(sql)
            rows = self.cursor.fetchall()
            self.cursor.close()
            self.cursor = None

        else:
            raise Exception('Only Selects allowed')
        logging.debug('Query Completed: {}'.format(datetime.datetime.now().time()))
        return rows

    def create_cur(self):
        """This method will get called to create a cusor if one does not exist for the instance

        Args:(self):

        Returns:
          None: None
        """
        if self.cursor is None:
            self.cursor = self.conn.cursor()

    def execute(self, sql, commit=False, catch_exception=True):
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
            "Debug DB Execute: {}:{}:{} \n\t{} ".format(self.userid, self.host, self.dbname, sql))
        rowcount = 0
        this_sql = str(sql).strip()

        if catch_exception:
            try:
                self.cursor.execute(this_sql)
            except Exception as e:
                # print("Error Execute SQL:{}".format(e))
                logging.warning("SQL error Occurred But Continuing:\n{}".format(e))
        else:
            self.cursor.execute(this_sql)
        rowcount = self.cursor.rowcount
        if commit:
            self.commit()

        logging.debug("DB SQL Execute Completed: {}".format(self.str))

        return rowcount

    def commit(self):
        pass

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


class ConnRDBMS(object):
    def __init__(self, autocommit=None, pwd=None, userid=None, host=None, dbname=None, schema=None):
        try:
            self.cursor = None
            self.autocommit = autocommit or True
            print('INIT DB: {}:{}:{}:{}:autocommit={}'.format(self.host, self.port, self.dbname, self.userid,
                                                              self.autocommit))
            logging.debug('Connect: {}:{}:{}'.format(self.host, self.dbname, self.userid))
        except Exception as e:
            logging.debug("Can not Use this Class directly: You must instantiate a child")
            sys.exit(1)
        self.str = 'DB: {}:{}:{}:{}:autocommit={}'.format(self.host, self.port, self.dbname, self.userid,
                                                          self.autocommit)

    def __repr__(self):
        return self.str

    def __str__(self):

        return self.str

    def __del__(self):
        logging.debug("Destroying: {}".format(self.str))
        self.close()

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
                return sqlalchemy.create_engine(self.sql_alchemy_uri.format(
                    userid=self.userid,
                    pwd=self.pwd,
                    host=self.host,
                    port=self.port,
                    db=self.dbname
                ))
            except Exception as e:
                logging.error("Could not Connect to sqlAlchemy, Check Uri Syntax: {}".format(e))
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
