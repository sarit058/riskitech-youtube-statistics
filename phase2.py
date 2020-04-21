import psycopg2

con = psycopg2.connect(
    host = "localhost",
    user = "postgres",
    password = "where564"
)

cur = con.cursor()


class mydatabase:
    def init_database:
        cur.execute("CREATE DATABASE youtube")


    def drop_database:
        cur.execute("DROP DATABASE youtube")


con.close()
