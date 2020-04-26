import psycopg2
import pandas as pd
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

## connection to postgres server
con = psycopg2.connect(
    host = "localhost",
    user = "postgres",
    database = "youtube",
    password = "where564"
)

con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

cur = con.cursor()


class mydatabase:
    def __init__(self, name):
        self.name = name
            
    def init_database(self):
        cur.execute("CREATE DATABASE {0!s}".format(self.name))
    def drop_database(self):
        cur.execute("DROP DATABASE {0!s}".format(self.name))
    
    def init_data(self, file_name):
        self.file_name = file_name[0:8]
        ## READING A CSV FILE
        country_videos = pd.read_csv(file_name)

        ## COLUMNS NAME AND TYPE
        column_name = country_videos.columns
        column_type =country_videos.dtypes.iloc

        ## CREATING LONG STRING "text": CREAT TABLE country_videos (text)
        for i in range (len(column_name)):
            if column_type[i] == 'object':
                column_type[i]='varchar'
                
            elif column_type[i] == 'int64':
                column_type[i]='int'
                
            if column_type[i] == 'bool':
                column_type[i]='bool'
                
            if column_type[i] == 'float64':
                column_type[i]='float'                      
            if i == 0:
                text = column_name[i] + ' ' + column_type[i]
            else:
                text = text + ', ' + column_name[i] + ' ' + column_type[i]
        ## CREATING THE TABLE
        cur.execute("CREATE TABLE {0!s} ({1!s});".format(self.file_name, text))
        # cur.execute("DROP TABLE {0!s}".format(self.file_name)) 


mdb = mydatabase('youtube')
# mdb.init_database()

mdb.init_data('USvideos.csv')

con.close()



    


