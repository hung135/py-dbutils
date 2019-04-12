
from ..world import ConnRDBMS
from ..world import DB
import sys
import os
import logging as lg
lg.basicConfig()
logging = lg.getLogger()
logging.setLevel(lg.INFO)

class DB(ConnRDBMS,DB):
    sql_alchemy_uri='postgresql://{userid}:{pwd}@{host}:{port}/{db}'


    def __init__(self,autocommit=None, pwd=None, userid=None, host=None, port=None,dbname=None,schema=None,label=None):
        import psycopg2
        self.autocommit=autocommit
        if pwd is None:
            self.pwd = os.getenv('PGPASSWORD', 'docker')
        if userid is None:
            self.userid = os.getenv('PGUSER', 'postgres')
        self.ssl = os.getenv('PGSSLMODE', 'prefer')
        if host is None:
            self.host = os.getenv('PGHOST', 'localhost')
        if port is None:
            self.port = os.getenv('PGPORT', 5432)
        if dbname is None:
            self.dbname = os.getenv('PGDATABASE', 'postgres')
        if label is None:
            self.label='py_dbutils'
        conn = psycopg2.connect(dbname=self.dbname, user=self.userid, password=self.pwd, port=self.port,
                                host=self.host, application_name=self.label, sslmode=self.ssl)
        conn.set_client_encoding('UNICODE')
        self.conn=conn
        #call the parent to set anything else we didn't set
        #super should check to see if the values are none
        super().__init__()

