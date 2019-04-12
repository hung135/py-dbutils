from ..world import ConnRDBMS
from ..world import DB
import psycopg2
import sys
import os
import logging as lg

lg.basicConfig()
logging = lg.getLogger()
logging.setLevel(lg.INFO)


class DB(ConnRDBMS, DB):

    sql_alchemy_uri = 'postgresql://{userid}:{pwd}@{host}:{port}/{db}'

    def __init__(self, autocommit=None, pwd=None, userid=None, host=None,
                 port=None, dbname=None, schema=None, label=None):
        self.ssl = os.getenv('PGSSLMODE', 'prefer')
        self.autocommit = autocommit
        self.pwd = pwd or os.getenv('PGPASSWORD', 'docker')
        self.userid = userid or os.getenv('PGUSER', 'postgres')
        self.host = host or os.getenv('PGHOST', 'localhost')
        self.port = port or os.getenv('PGPORT', 5432)
        self.dbname = dbname or os.getenv('PGDATABASE', 'postgres')
        self.label = label or 'py_dbutils'

        conn = psycopg2.connect(dbname=self.dbname, user=self.userid, password=self.pwd, port=self.port,
                                host=self.host, application_name=self.label, sslmode=self.ssl)
        conn.set_client_encoding('UNICODE')
        self.conn = conn
        # call the parent to set anything else we didn't set
        # super should check to see if the values are none
        print("Initialize Super")
        super().__init__()

