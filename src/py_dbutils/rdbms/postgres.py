from ..parents import ConnRDBMS
from ..parents import DB as DATABASE
import psycopg2
import sys
import os
import logging as lg

lg.basicConfig()
logging = lg.getLogger(__name__)



class DB(ConnRDBMS, DATABASE):
    sql_alchemy_uri = 'postgresql://{userid}:{pwd}@{host}:{port}/{db}?application_name={appname}'

    def __init__(self, autocommit=None, pwd=None, userid=None, host=None,
                 port=None, dbname=None, schema=None, label=None, loglevel=None):
        
        logging.level=loglevel
        self.ssl = os.getenv('PGSSLMODE', 'prefer')
        self.autocommit = autocommit
        self.pwd = pwd or os.getenv('PGPASSWORD', 'docker')
        self.userid = userid or os.getenv('PGUSER', 'postgres')
        self.host = host or os.getenv('PGHOST', 'localhost')
        self.port = port or os.getenv('PGPORT', 5432)
        self.dbname = dbname or os.getenv('PGDATABASE', 'postgres')
        self.label = label or __file__
        self.schema = schema
        self.appname=self.label
        conn = psycopg2.connect(dbname=self.dbname, user=self.userid, password=self.pwd, port=self.port,
                                host=self.host, application_name=self.label, sslmode=self.ssl)
        conn.set_client_encoding('UNICODE')
        self.conn = conn
        # call the parent to set anything else we didn't set
        # super should check to see if the values are none
        logging.debug("Initialize Parent Class")
        super().__init__(loglevel=loglevel)

    # copy using pyscopg to convert a dataframe to a file like object and pass it into pyscopg
    # this does not write to the file system but puts all the data into memory
    def bulk_load_dataframe(self, dataframe, table_name_fqn, encoding='utf8', workingpath='MEMORY'):
        """Takes a dataframe and converts to a memory file or local file the import into table

        Args:( ):
          dataframe (dataframe): String
          table_name_fqn (str): String
          encoding (str) ='utf8'
          workingpath (str) = MEMORY / Directory Path
        Returns:
          None: None
        """
        try:
            from StringIO import StringIO
        except ImportError:
            from io import StringIO

        from contextlib import contextmanager

        @contextmanager
        def readStringIO():

            # from cStringIO import StringIO
            try:
                # make sure we are at the begining of the object/file
                data_stringIO = StringIO()
                dataframe.to_csv(data_stringIO, header=False,
                                 index=False, encoding='utf8')
                data_stringIO.seek(0)
                yield data_stringIO
            finally:
                data_stringIO.close()

        self.create_cur()
        column_list = ['"{}"'.format(c)
                       for c in dataframe.columns.values.tolist()]
         
        if workingpath == 'MEMORY':
            with readStringIO() as f:

                cmd_string = """COPY {table} ({columns}) FROM STDIN WITH (FORMAT CSV)""".format(table=table_name_fqn,
                                                                                                columns=','.join(
                                                                                                    column_list))
                self.cursor.copy_expert(cmd_string, f)
        else:
            tmp_file = os.path.join(workingpath, '_tmp_file.csv')
            dataframe.to_csv(tmp_file, header=False,
                             index=False, encoding='utf8')
             
            with open(tmp_file) as f:
                self.cursor.copy_from(
                    f, table_name_fqn, columns=column_list, sep=",")
    def get_primary_keys(self, table_name_fqn):
        

        sql="""SELECT a.attname
        FROM   pg_index i
        JOIN   pg_attribute a ON a.attrelid = i.indrelid
                            AND a.attnum = ANY(i.indkey)
        WHERE  i.indrelid = '{}'::regclass
        AND    i.indisprimary; """.format(table_name_fqn)
            

        rows, meta = self.query(sql)
        # will fail if we get no rows
        
        return [str(r[0]) for r in rows]