from ..world import ConnRDBMS
from ..world import DB
import sqlalchemy
import sys
import os
import logging as lg
import jaydebeapi

lg.basicConfig()
logging = lg.getLogger()
logging.setLevel(lg.INFO)


class DB(ConnRDBMS, DB):

    #https://stackoverflow.com/questions/25596737/working-with-an-access-database-in-python-on-non-windows-platform-linux-or-mac
    sql_alchemy_uri ='access:///{file_path}'
    ucanaccess_jars = [
        "./jdbc_jar/ucanaccess-4.0.2.jar",
        "./jdbc_jar/commons-lang-2.6.jar",
        "./jdbc_jar/commons-logging-1.1.3.jar",
        "./jdbc_jar/hsqldb-2.3.1.jar",
        "./jdbc_jar/jackcess-2.1.6.jar",
    ]
    classpath = ":".join(os.path.abspath(ucanaccess_jars))


    #https://dev.mysql.com/doc/refman/8.0/en/environment-variables.html
    def __init__(self,  file_path,autocommit=None):

        self.file_path=os.path.abspath(file_path)
        self.autocommit = autocommit
        print(self.classpath)
        self.conn = jaydebeapi.connect(
        "net.ucanaccess.jdbc.UcanaccessDriver",
        "jdbc:ucanaccess://{file_path};newDatabaseVersion=V2010".format(self.file_path),
        ["", ""],
        classpath
                )
        self.cursor=None

        self.str = 'DB: MSAccess:{}:autocommit={}'.format(file_path,self.autocommit)

    def connect_SqlAlchemy(self):
        if self.sql_alchemy_uri is None:
            logging.error("sqlAlchemy not supported for this DB")
            sys.exit(1)
        else:
            try:
                return sqlalchemy.create_engine(self.sql_alchemy_uri.format(file_path=self.file_path))

            except Exception as e:
                logging.error("Could not Connect to sqlAlchemy, Check Uri Syntax: {}".format(e))
                sys.exit(1)

