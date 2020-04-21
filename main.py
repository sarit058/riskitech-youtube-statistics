import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


con = psycopg2.connect(
    host = "localhost",
    user = "postgres",
    password = "where564"
)

con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

cur = con.cursor()

class mydatabase:
    def __init__(self, name):
        self.name = name
    def init_database(self):
        cur.execute("CREATE DATABASE {0!s}".format(mdb.name))
    def drop_database(self):
        cur.execute("DROP DATABASE {0!s}".format(mdb.name))
mdb = mydatabase('youtube')
mdb.init_database()


con.close()


