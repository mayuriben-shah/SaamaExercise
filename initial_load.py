from ctypes import util
import pandas as pd
from datetime import datetime
import sys
import time
from common_util import Common_Utils
import io

class first_load:
    def __init__(self,conn,conn_engine,filename, env,logger): #connection object, prebuilt queries, tosql may need to accept connection 
        """
        first_load class is responsible for taking a first file and Load First file to table
         
        :param conn_engine: (psycopg2 connection engine object)
        :param newFile: (str)
        :param logger: (logger) log class
        :param env: (dict) contain config info
        :param keys: (list) contains keys in string format
        """ 
        self.conn_engine = conn_engine
        self.filename = filename
        self.df = None
        self.conn = conn
        
        self.env = env
        self.logger = logger   
        self.util = Common_Utils()
        self.columns = self.util.get_columns(self.filename)
        self.schematable = self.env['schema'] + "." + self.env['table']
                 
    def create_colstring(self):
        colstring = ""
        last_col = self.columns[-1]
        
        for col in self.columns:
            if col == last_col:
                colstring = colstring + col + " text NULL"
            else :
                colstring = colstring + col + " text NULL, "
        return colstring

    def create_table(self,colstring,tablename):
        cur = self.conn.cursor()
        query = self.util.get_jsql_query("create_dynamic_table.jsql",parameters={"schemaTable":tablename,"columnwithdatatype":colstring})
        cur.execute(query)
        self.conn.commit()
        print("table created")

    
    def insert_firstload_data(self):
        """
        Read the CSV file and Do the initial load to database.
        """
        try:
            print("initial load")
            colstring = self.create_colstring()
            colstringTable = colstring 
            self.create_table(colstringTable,self.schematable)

            self.df = pd.read_csv(self.filename,low_memory=False,dtype=object,chunksize=150000)
            self.current_date =  datetime.now().strftime("%m%d%Y%H%M%S")       
            t1 = time.time()
            self.counter = 0
            for chunk in self.df:
                if self.counter == 0:
                    self.if_exists = "replace"
                    self.counter =  1
                else:
                    self.if_exists = "append"
                sbuf = io.StringIO()
                chunk.to_csv(sbuf, encoding="utf-8", index=False)
                sbuf.seek(0)
                cur = self.conn.cursor()
                copy_sql = """
                COPY %s FROM stdin WITH CSV HEADER
                DELIMITER as ','
                """
                cur.copy_expert(sql=copy_sql % (self.env["schema"] + "." + self.env["table"]), file=sbuf)
            load_time = time.time() - t1
            print("COPY duration: {} seconds".format(load_time))
            self.logger.log(self.logger.DEV,'Time To Load: ' + str(load_time))
            self.logger.log(self.logger.DEV,'initial load completed')
        except Exception as e:
            self.logger.log(self.logger.ERROR, e)
            sys.exit(1)
        