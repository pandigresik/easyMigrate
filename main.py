#!/usr/bin/python
from dbf6 import Dbf6
from sqlalchemy import create_engine
import os
import logging
import time
import sys
import getopt

logging.basicConfig(filemode='a',filename='app.log',level='DEBUG')

logger = logging.getLogger(__name__)
# Misc logger setup so a debug log statement gets printed on stdout.

handler = logging.StreamHandler()
log_format = "%(asctime)s %(levelname)s -- %(message)s"
formatter = logging.Formatter(log_format)
handler.setFormatter(formatter)
logger.addHandler(handler)

conn_string = None #'mysql://user:password@localhost/bt1'
directory = None #'/data/docker/wjc/BT1_DBF/'
create_table = False
argument_list = sys.argv[1:]
short_options = "c:d:o"
long_options = ["conn=", "dir=", "output-schema"]
try:
    arguments, values = getopt.getopt(argument_list, short_options, long_options)
    for current_argument, current_value in arguments:
        if current_argument in ("--conn"):
            conn_string = current_value
        elif current_argument in ("--dir"):
            directory = current_value
        elif current_argument in ("--output-schema"):
            create_table = True
except getopt.error as err:
    print(err)
    sys.exit(2)

if conn_string is None:
    print("koneksi string belum didefinisikan")
    sys.exit()
if directory is None:
    print("direktory file dbf belum didefinisikan")
    sys.exit()

column_type = {
        'C': 'varchar',
        'N': 'float',
        'L': 'char',
        'D': 'date',
        'M': 'text',
        'F': 'double',
        'B': 'decimal',
        'G': 'blob',
        'P': 'blob',
        'Y': 'decimal(21,4)',
        'T': 'datetime',
        'I': 'int'
    }

def convert_column(column: tuple):
    name, type, length = column    
    
    if type in ['D','M','G','P','Y','T']:
        str_result = "{} {}".format(name,column_type[type])
    elif type in ['N','F','B']:
        str_result = "{} {} ({},2) ".format(name,column_type[type],length+2)
    else:
        str_result = "{} {} ({}) ".format(name,column_type[type],length)
        
    return str_result

def create_schema_table(table_name: str, fields: list):
    return """
create table {} (
{}
);
    """.format(table_name, ",\n".join(fields))
    
def write_file(pathfile, data: str):
    f = open(pathfile, 'w')
    f.write(data)
    f.close()


# '/data/docker/wjc/ADM_DBF/BDDRHK.DBF'
def read_dbf(con, dbf_file):
    rhk = None
    try:
        rhk = Dbf6(dbf_file, codec='utf-8')
        #logger.info('read '+dbf_file+' success with utf-8 codec')
    except Exception as err:
        logger.error(err)
        try:
            logger.info('read again '+dbf_file+' with latin codec')
            rhk = Dbf6(dbf_file, codec='latin')
            #logger.info('read '+dbf_file+' success with latin codec')
        except Exception as err:
            logger.error('latin codec ')
            logger.error(err)
        
    return rhk
def generate_schema_table(obj_dbf, table_name):
    desc_fields = obj_dbf.fields        
    fields = [convert_column(data) for index,data in enumerate(desc_fields) if index > 0]
    return create_schema_table(table_name, fields)
    
def copy_table(con, obj_dbf, table_name):
    try:
        con.execute("""TRUNCATE TABLE %s """ % (table_name))
    except Exception as err:
        logger.error(err)
    
    try:        
        df = obj_dbf.to_dataframe()
        df.to_sql(table_name,con = con, if_exists = 'append', index = False, chunksize=10000)
    except Exception as err:
        logger.error(err)            
        

def copy_database(directory, create_table = False):
    conn = create_engine(conn_string)
    first_start = time.time()
    schema_arr = []
    for filename in os.listdir(directory):
        if filename.endswith(".dbf") or filename.endswith(".DBF"):
            if filename[0].isalpha():
                start = time.time()
                tablename = filename[:-4]
                pathfile = os.path.join(directory,filename)
                try:
                    obj_dbf = read_dbf(con=conn, dbf_file=pathfile)
                    if create_table:
                        if obj_dbf is not None:
                            schema_arr.append(generate_schema_table(obj_dbf, table_name = tablename))
                    else:
                        if obj_dbf is not None:
                            copy_table(con=conn, obj_dbf=obj_dbf, table_name=tablename)
                            
                    end = time.time()
                    logger.info("execute {} ran in {}s".format(pathfile, round(end - start, 2)))
                except Exception as error:
                    logger.info("execute {} failed".format(pathfile))
                    logger.error(error)                    
        else:
            continue

    if len(schema_arr) > 0:
        write_file('schema_database.sql',''.join(schema_arr))
    last_end = time.time()
    logger.info("execute all file in {}s".format(round(last_end - first_start, 2)))
    conn.dispose()


if __name__ == "__main__":
    copy_database(directory, create_table)