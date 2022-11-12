import sqlite3

class dbactions_perform:
    def create_db(self,dbname='master.db'):
        try:
            conn = sqlite3.connect(dbname)
            cur = conn.cursor()
            return cur,conn
        except:
            raise Exception("DB creation error")


    def create_table(self,cur,conn):
        try:
            cur.execute('''CREATE TABLE sports 
                     (name text, data_of_birth date, us_state text, company_name text, last_active date , score int,
                                   member_since int , sport  text)''')
            conn.commit()
            return True
        except:
            raise Exception("Table creation error")

    def insert_db(self,cur,conn):
        try:
            cur.execute('''"INSERT INTO stocks VALUES ()"''')
            conn.commit()
            conn.close()
        except:
            raise Exception("Table creation error")
