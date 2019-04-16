from ..parents import ConnRDBMS
from ..parents import DB

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
    #https://dev.mysql.com/doc/refman/8.0/en/environment-variables.html
    def __init__(self, autocommit=None, pwd=None, userid=None, host=None, port=None, dbname=None, schema=None,
                 label=None):

        self.autocommit = autocommit
        self.pwd = pwd or os.getenv('MYSQL_PWD', 'docker')
        self.userid=userid or os.getenv('USER', 'root')
        self.host = host or os.getenv('PGHOST', 'localhost')
        self.port = port or os.getenv('MYSQL_TCP_PORT', 3306)
        self.dbname = dbname or os.getenv('DATABASE', 'mysql')
        self.label = label or 'py_dbutils'


        conn = pymysql.connect(user=self.userid, password=self.pwd,
                               host=self.host, port=int(port),
                               database=self.dbname)
        self.conn = conn

        # call the parent to set anything else we didn't set
        # super should check to see if the values are none
        super().__init__()
