from ..parents import ConnRDBMS, ConnREST
from ..parents import DB
import sqlalchemy
import sys
import os
import logging as lg
import pybigquery

lg.basicConfig()
logging = lg.getLogger(__name__)



class DB(ConnREST, DB):
    # get from here:
    # https://docs.sqlalchemy.org/en/latest/core/engines.html
    sql_alchemy_uri = 'bigquery://{project}'

    # https://dev.mysql.com/doc/refman/8.0/en/environment-variables.html
    def __init__(self, project, data_id, key_file, autocommit=None):

        self.file_path = os.path.abspath(key_file)
        self.autocommit = autocommit
        self.project = project
        self.data_id = data_id
        self.key_file = key_file

        self.str = 'DB: BIGQuery:{}:{}:autocommit={}'.format(project, data_id, self.autocommit)

    def connect_SqlAlchemy(self):
        if self.sql_alchemy_uri is None:
            logging.error("sqlAlchemy not supported for this DB")
            sys.exit(1)
        else:
            try:
                return sqlalchemy.create_engine(self.sql_alchemy_uri.format(file_path=self.project))

            except Exception as e:
                logging.error("Could not Connect to sqlAlchemy, Check Uri Syntax: {}".format(e))
                sys.exit(1)

# df = pd.read_gbq(query['query'],
#                project_id=query['project_id'],
#                dataset_id=query['dataset_id'],
#                private_key=SERVICE_ACCOUNT_FILE, dialect='standard')
