from jinjasql import JinjaSql
import logging
import os
import csv
import sys

j = JinjaSql()

class Common_Utils(object):
    # Function to retrieve the SQL query for the give file name and parameters
    def get_jsql_query(self, query_id, parameters):
        try:
            query_path = os.path.normpath(
                os.path.dirname(__file__) + "/query/" + query_id
            )
            #query_path = "C:/Users/mayuriben.shah.saama/Desktop/incremntal_dataload-review/query/" + query_id
            #print(query_path)
            with open(f"{query_path}", "r") as f:
                query_text = f.read()
                query, bind_params = j.prepare_query(query_text, parameters)
            return query
        except Exception as e:
            # logging.error(
            #     f"SQL retrieval failed for query_id {query_id} with error {e}"
            # )
            raise e

    def executesql(self, connection_detail, query, select=False):
        try:
            if select:
                return connection_detail.execute(query)
            else:
                connection_detail.execute(query)
        except Exception as e:
            # logging.error(f"Execution of query {query} with error {e}")
            raise e

    def get_columns(self,path):
        try:
            row = None
            with open(path) as f:
                reader = csv.reader(f)
                row = next(reader)   
            return row
        except Exception as e:
            self.logger.log(self.logger.ERROR, e)
            sys.exit(1)

    def get_insert_colstring(self,columns):
        colstring = ""
        for col in columns:
            colstring = colstring + col + ","
        return colstring

   