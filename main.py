import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import pandas as pd
import json
from pandas import json_normalize

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
      
        
            cur.execute("INSERT INTO {0!s} ({1!s}) VALUES ({2!s});".format(self.file_name,  column_text, astr))


    def del_data(self, file_name):
        self.file_name = file_name[0:8]

        ## DELETING THE TABLE
        cur.execute("DROP TABLE {0!s}".format(self.file_name))

    def init_json(self, j_file_name):
        self.j_file_name = j_file_name[0:14]

        ## READ JSON FILE
        with open(j_file_name, 'rb') as json_file:
            text = json.load(json_file)
        
        ## CREATING 'CATEGORY_ID' DATAFRAME
        json_file = pd.DataFrame(json_normalize(text, 'items'))
        json_file = json_file.set_index('id')
        country_video_categories_titles =  pd.DataFrame(json_file['snippet.title'])
        country_video_categories_titles.index.name = 'category_id'
        country_video_categories_titles = country_video_categories_titles.rename(columns={'snippet.title': 'title'})
        country_video_categories_titles.index = country_video_categories_titles.index.values.astype(int)
        country_video_categories_titles['title'] = country_video_categories_titles['title'].astype(str)
       
        ## CREATING THE TABLE
        cur.execute("CREATE TABLE {0!s} (id int NOT NULL PRIMARY KEY, category varchar);".format(self.j_file_name))
        
        ## DELETING THE TABLE
        # cur.execute("DROP TABLE {0!s};".format(self.j_file_name))
        

        ## INSERTING DATA INTO THE TABLE
        for i in range(len(country_video_categories_titles.index)):
            catstr = str(country_video_categories_titles.index[i]) + ", '" + country_video_categories_titles.values[i][0] + "'" 
                          
            cur.execute("INSERT INTO {0!s} VALUES ({1!s});".format(self.j_file_name, catstr))
        
        ## FORIEN KEY IN CountryVideos
        
        




mdb = mydatabase('youtube')
# mdb.init_database()

# mdb.init_data('USvideos.csv')
# mdb.del_data('USvideos.csv')

mdb.init_json('US_category_id.json')

con.close()



    


