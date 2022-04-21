import sys
import os
import configparser as cp
import psycopg2
from sqlalchemy import create_engine
from utilities.logger import Logger as logger
from common_util import Common_Utils
from initial_load import first_load


# set config file
CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),"config.properties")
# log dir
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),'logs')

class Driver:
    """
    This class is the driving class for checking have you done intial load or not?
    """
    def __init__(self, fp, config, logger):
        """
        :param fp: (str) filepath of the file to be uploaded or incrementally loaded
        :param keys: (list) business keys of the file in a list of strings 
        :param config: (RawConfigParser) parser class used to read environment variables
        :param logger: (logger) log class
        """
        self.filepath = fp
        self.config = config
        self.logger = logger
        self.env = {
            'postgres_host':config.get('metadata','HOST'),
            'database':config.get('metadata','DATABASE'),
            'username':config.get('metadata','USERNAME'),
            'password':config.get('metadata','PASSWORD'),
            'schema':config.get('metadata','SCHEMA'),
            'table':config.get('metadata','TABLE'),
            'port':config.get('metadata','PORT'),
        }
        self.util = Common_Utils()
        
        try:
            self.conn = psycopg2.connect(database=self.env['database'], 
                                        user=self.env['username'], 
                                        password=self.env['password'], 
                                        host=self.env['postgres_host'], 
                                        port=self.env['port'])
        except Exception as e:
            self.logger.log(self.logger.ERROR, e)
            sys.exit(1)

        try:
            postgres_host = self.env['postgres_host']
            database = self.env['database']
            username = self.env['username']
            password = self.env['password']
            connection_string = 'postgresql://' + username + ':' + password + '@' + \
            postgres_host + '/' + database
            db = create_engine(connection_string, pool_pre_ping=True)
            self.conn_engine = db.connect()
        except Exception as e:
            self.logger.log(self.logger.ERROR, e)
            sys.exit(1)

        
        try:
            with self.conn:
                cursor = self.conn.cursor()
                query = self.util.get_jsql_query('check_table.jsql',parameters={"table_schema":self.env['schema'],"table_name":self.env['table']})
                cursor.execute(query)
               
                if not cursor.fetchone()[0]:
                    cursor.close()
                    print('Start Initial load')
                    trans = self.conn_engine.begin()
                    self.first_load_object = first_load(self.conn,self.conn_engine,fp, self.env,self.logger)
                    self.first_load_object.insert_firstload_data()
                    trans.commit()
                else:
                    print("you have already done initial load")
                    
        except Exception as e:
            self.logger.log(self.logger.ERROR, e)
            sys.exit(1)
    


if __name__ == "__main__":
    config = cp.RawConfigParser()
    config.read(CONFIG_FILE)   

    # initiate logger
    log = logger(
            # module name
            'Load', 
            # log file_path
            LOG_DIR,
            # config
            config,
        )

    if len(sys.argv) != 2:
        sys.exit("Wrong number of arguments!")

    filepath = sys.argv[1]
    load_driver = Driver(filepath, config, log)
    load_driver.logger.close_log()
    sys.exit(0)


