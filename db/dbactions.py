import sqlite3
import pandas as pd
def create_db(dbname='master.db'):
    try:
        conn = sqlite3.connect(dbname)
        cur = conn.cursor()
        return cur, conn
    except:
        raise Exception("DB creation error")

def create_table(cur, conn):
    try:
        cur.execute('''CREATE TABLE sports
                 (name text, data_of_birth date, us_state text, last_active date , score int,
                               joined_league int , sport  text, company_name text)''')
        conn.commit()
        return True
    except:
        raise Exception("Table creation error")


def insert_db(cur, conn, data_to_insert):
    try:
        data_to_insert.to_sql('sports', conn, if_exists='append', index=False)
        pd.read_sql('select * from sports', conn)
        conn.commit()
        conn.close()

    except:
        raise Exception("Table creation error")
