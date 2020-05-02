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
                column_text = column_name[i]
            else:
                text = text + ', ' + column_name[i] + ' ' + column_type[i]
                column_text = column_text + ', ' + column_name[i]
            
        ## CREATING THE TABLE
        cur.execute("CREATE TABLE {0!s} ({1!s});".format(self.file_name, text))
        
        
        ## INSERTING DATA INTO THE TABLE
        for i in range(len(country_videos.index)):
            a = country_videos.values[i].tolist()
            
            # CHECK FOR BAD CHARACTER: '
            for j in range(len(column_name)):
                if type(a[j]) == str:
                    a[j] = a[j].replace("'", '')
        
            seperator = "', '"
            astr ="'" + seperator.join(str(x) for x in a) + "'"
      
        
            cur.execute("INSERT INTO {0!s} VALUES ({2!s});".format(self.file_name,  column_text, astr))


    def del_data(self, file_name):
        self.file_name = file_name[0:8]

        ## DELETING THE TABLE
        cur.execute("DROP TABLE {0!s}".format(self.file_name))


mdb = mydatabase('youtube')
# mdb.init_database()

mdb.init_data('USvideos.csv')
# mdb.del_data('USvideos.csv')

con.close()



    


