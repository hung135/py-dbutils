from ..parents import ConnRDBMS
from ..parents import DB as PARENTDB

import sys
import os
import logging as lg


lg.basicConfig()
logging = lg.getLogger(__name__)



class DB(ConnRDBMS, PARENTDB):

    # get from here:
    # https://docs.sqlalchemy.org/en/latest/core/engines.html
    sql_alchemy_uri = 'mssql+pymssql://{userid}:{pwd}@{host}:{port}/{db}'

    # https://dev.mysql.com/doc/refman/8.0/en/environment-variables.html
    def __init__(self, autocommit=None, pwd=None, userid=None, host=None, port=None, dbname=None, schema=None,
                 label=None,loglevel=None):
        logging.level=loglevel
        
        import pymssql  # putting here so we don't have to install it

        self.autocommit = autocommit
        self.pwd = pwd or os.getenv('MS_PASSWORD', 'Docker1234')
        self.userid = userid or os.getenv('MS_USER', 'sa')
        self.host = host or os.getenv('MS_HOST', 'localhost')
        self.port = port or os.getenv('MS_PORT', 11433)
        self.dbname = dbname or os.getenv('MS_DATABASE', 'master')
        self.label = label or 'py_dbutils'

        conn = pymssql.connect(user=self.userid, password=self.pwd,
                               host=self.host, port=port,
                               database=self.dbname)
        self.conn = conn

        # call the parent to set anything else we didn't set
        # super should check to see if the values are none
        super().__init__()

    def get_table_columns(self, table_name):
        """This method will select 1 record from the table and return the column names

                Args:(table_name (fully qualified):

                Returns:
                    List: List of Strings
                """
        sql = """select top 1 * from {} """.format(table_name)

        rows, meta = self.query(sql)
        # will fail if we get no rows
        return [str(r[0]) for r in meta]
