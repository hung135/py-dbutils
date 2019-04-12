import sys
import os
import logging as lg

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
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __repr__(self):
        pass

    def __str__(self):
        pass

    def __del__(self):
        pass

    def query(self):
        pass

    def execute(self):
        pass

    def commit(self):
        pass

    def bulk_load(self):
        pass


class ConnRDBMS(object):
    def __init__(self, autocommit=True, pwd=None, userid=None, host=None, dbname=None, schema=None):
        try:
            if self.autocommit is None:
                self.autocommit = autocommit
            if self.pwd is None:
                self.pwd
            print(' init DB: {}:{}:{}:{}:autocommit={}'.format(self.host, self.port, self.dbname, self.userid,
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
