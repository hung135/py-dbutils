from ..parents import ConnRDBMS
from ..parents import DB
import sqlalchemy
import sys
import os
import logging as lg
import sqlite3

lg.basicConfig()
logging = lg.getLogger()
logging.setLevel(lg.INFO)


class DB(ConnRDBMS, DB):
    # get from here:
    # https://docs.sqlalchemy.org/en/latest/core/engines.html
    sql_alchemy_uri = 'sqlite:///{file_path}'

    # https://dev.mysql.com/doc/refman/8.0/en/environment-variables.html
    def __init__(self, file_path, autocommit=None):

        self.file_path = os.path.abspath(file_path)
        self.autocommit = autocommit

        self.conn = sqlite3.connect(self.file_path)
        self.cursor = None

        self.str = 'DB: SQLITE:{}:autocommit={}'.format(file_path, self.autocommit)

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
