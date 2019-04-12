from ..world import ConnRDBMS
from ..world import DB

import sys
import os
import logging as lg
import pymysql

lg.basicConfig()
logging = lg.getLogger()
logging.setLevel(lg.INFO)


class DB(ConnRDBMS, DB):
    #get from here:
    #https://docs.sqlalchemy.org/en/latest/core/engines.html
    sql_alchemy_uri ='mysql+pymysql://{userid}:{pwd}@{host}:{port}/{db}'

    def __init__(self, autocommit=None, pwd=None, userid=None, host=None, port=None, dbname=None, schema=None,
                 label=None):
        import psycopg2
        self.autocommit = autocommit
        if pwd is None:
            self.pwd = os.getenv('PGPASSWORD', 'docker')
        if userid is None:
            self.userid = os.getenv('PGUSER', 'root')
        self.ssl = os.getenv('PGSSLMODE', 'prefer')
        if host is None:
            self.host = os.getenv('PGHOST', 'localhost')
        if port is None:
            self.port = os.getenv('PGPORT', 3306)
        if dbname is None:
            self.dbname = os.getenv('PGDATABASE', 'mysql')
        if label is None:
            self.label = 'py_dbutils'


        conn = pymysql.connect(user=self.userid, password=self.pwd,
                               host=self.host, port=port,
                               database=self.dbname)
        self.conn = conn
        self.sqlAlchemyUrl=''
        # call the parent to set anything else we didn't set
        # super should check to see if the values are none
        super().__init__()
