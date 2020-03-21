from ..parents import ConnRDBMS 
from ..parents import DB as ParentDB
import sqlalchemy
import sys
import os
import logging as lg
import jaydebeapi
import pprint

lg.basicConfig()
logging = lg.getLogger(__name__)



class DB(ConnRDBMS, ParentDB):

    # https://stackoverflow.com/questions/25596737/working-with-an-access-database-in-python-on-non-windows-platform-linux-or-mac
    #sql_alchemy_uri ='access:///{file_path}'
    sql_alchemy_uri = None

    ucanaccess_jars = [
        "./jdbc_jar/ucanaccess-4.0.2.jar",
        "./jdbc_jar/commons-lang-2.6.jar",
        "./jdbc_jar/commons-logging-1.1.3.jar",
        "./jdbc_jar/hsqldb-2.3.1.jar",
        "./jdbc_jar/jackcess-2.1.6.jar",
    ]
    classpath = ":".join(os.path.abspath(os.path.join(
        os.path.dirname(__file__), i)) for i in ucanaccess_jars)

    # https://dev.mysql.com/doc/refman/8.0/en/environment-variables.html
    def __init__(self,  file_path, autocommit=None,loglevel=None):
        logging.level=loglevel
        

        self.file_path = os.path.abspath(file_path)
        self.autocommit = autocommit

        # need to set this for attempting connection
        self.str = 'DB: MSAccess:{}:autocommit={}'.format(
            file_path, self.autocommit)

        self.conn = jaydebeapi.connect(
            "net.ucanaccess.jdbc.UcanaccessDriver",
            "jdbc:ucanaccess://{file_path};".format(file_path=self.file_path),
            ["", ""],
            self.classpath
        )
        self.cursor = None

    def connect_SqlAlchemy(self):

        raise Exception('sqlAlchemy not supported for MSAccess')

    def get_all_tables(self):
        results = self.conn.jconn.getMetaData().getTables(None, None, "%", None)
        table_reader_cursor = self.conn.cursor()
        table_reader_cursor._rs = results
        table_reader_cursor._meta = results.getMetaData()
        read_results = table_reader_cursor.fetchall()
        return [row[2] for row in read_results if row[3] == 'TABLE']
