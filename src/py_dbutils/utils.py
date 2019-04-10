import logging


def purge_schema_except(db, schema_list):
    # this will run through and drop all schemas except for those in the schema_list
    sql_all_schemas = ''
    drop_schema_list = []
    sql_drop_schema = """DROP schema {} """
    dbtype = db._dbtype

    if dbtype in ['POSTGRES', 'MAPD']:
        sql_all_schemas = """SELECT schema_name from information_schema.schemata"""
        sql_drop_schema = """DROP schema {} CASCADE"""
    if dbtype in ['ORACLE']:
        sql_all_schemas = """SELECT DISTINCT USERNAME from SYS.ALL_USERS"""
    if dbtype in ['MSSQL']:
        sql_all_schemas = """SELECT schema_name FROM sys.schemas"""
    if dbtype in ['VERTICA']:
        sql_all_schemas = """SELECT schema_name FROM sys.schemas"""
    if dbtype in ['SQLITE']:
        sql_all_schemas = """SELECT table_schema FROM v_catalog.tables"""
    if sql_all_schemas == '':
        raise Exception('Database Not Supported: {}'.format(db._dbtype))
    else:
        rs = db.query(sql_all_schemas)
        for row in rs:
            schema_name = row[0]
            if schema_name not in schema_list:
                drop_schema_list.append(schema_name)
        print("These schemas will be dropped, if you answer 'yess!!!'")
        print(drop_schema_list)

        reply = raw_input("Are you Sure? Type: yes!!! >")
        if reply == 'yes!!!':
            for schema_name in drop_schema_list:
                try:
                    db.execute(sql_drop_schema.format(schema_name))
                    logging.info("Dropped Schema: {}".format(schema_name))
                except Exception as e:
                    logging.error("Error Dropping Schema: {}\n\t{}".format(schema_name, e))
        else:
            logging.info('Nothing was Done!')


def get_schema_except(db, execpt_schema_list=[]):
    # this will run through and drop all schemas except for those in the schema_list
    sql_all_schemas = ''
    schema_list = []
    sql_drop_schema = """DROP schema {} """
    dbtype = db._dbtype

    if dbtype in ['POSTGRES', 'MAPD']:
        sql_all_schemas = """SELECT schema_name from information_schema.schemata"""
        sql_drop_schema = """DROP schema {} CASCADE"""
    if dbtype in ['ORACLE']:
        sql_all_schemas = """SELECT DISTINCT USERNAME from SYS.ALL_USERS"""
    if dbtype in ['MSSQL']:
        sql_all_schemas = """SELECT schema_name FROM sys.schemas"""
    if dbtype in ['VERTICA']:
        sql_all_schemas = """SELECT schema_name FROM sys.schemas"""
    if dbtype in ['SQLITE']:
        sql_all_schemas = """SELECT table_schema FROM v_catalog.tables"""
    if sql_all_schemas == '':
        raise Exception('Database Not Supported: {}'.format(db._dbtype))
    else:
        rs = db.query(sql_all_schemas)
        for row in rs:
            schema_name = row[0]
            if schema_name not in execpt_schema_list:
                schema_list.append(schema_name)

    return schema_list
# adds a column to a table in datbase


def add_column(db, table_name, column_name, data_type, nullable=''):
    data_type_formatted = ''
    if data_type == "Integer":
        data_type_formatted = "INTEGER"
    elif data_type == "String":
        data_type_formatted = "VARCHAR(100)"
    elif data_type == "uuid":
        data_type_formatted = "UUID"

    base_command = ("ALTER TABLE {table_name} ADD column {column_name} {data_type} {nullable}")
    sql_command = base_command.format(table_name=table_name, column_name=column_name, data_type=data_type_formatted,
                                      nullable=nullable)

    db.execute(sql_command)
