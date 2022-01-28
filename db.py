import os
import json
import datetime 
import mysql.connector
from dotenv import load_dotenv 
from os.path import join, dirname 


dotenv_path = join(dirname(__file__), './.env')
load_dotenv(dotenv_path)

DATABASE_HOST = os.environ.get("DATABASE_HOST")
DATABASE_NAME = os.environ.get("DATABASE_NAME")
DATABASE_USER = os.environ.get("DATABASE_USER")
DATABASE_PORT = os.environ.get("DATABASE_PORT")
DATABASE_PASSWORD = os.environ.get("DATABASE_PASSWORD")

class Config:
    def __init__(self):
        self.database = mysql.connector.connect(
        host= DATABASE_HOST,
        user = DATABASE_USER,
        password = DATABASE_PASSWORD,
        database = DATABASE_NAME,
        charset = 'utf8'
        )
        self.curs = self.database.cursor(dictionary=True, buffered=True)
        

    def execute(self, query):
        self.curs.execute(query)
        self.database.commit()
    

    def db_close(self):
        self.database.close()

    def execute_select_all(self, query):
        self.curs.execute(query)
        sql_result = self.curs.fetchall()
        json_dumps = json.dumps(sql_result, default=str)
        json_result = json.loads(json_dumps)
        return json_result
