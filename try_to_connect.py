import psycopg2
#from getpass import getpass
import pandas as pd



class my_table:
    def __init__(self):
        
        self.db_host = input("Plz enter the db host:")
        self.db_user = input("Plz enter the db user:")
        self.db_pass = input("Plz enter the psw:")
        self.db_name = input("Plz enter the db name:")
        self.db_schema = input("Plz enter the db schema:")
        
        

    def get_connected(self):
        self.conn = psycopg2.connect(
               host = self.db_host,
               dbname = self.db_name,
               user = self.db_user,
               password = self.db_pass,
               #schema = self.db_schema, 
               port = 5432
            )

        return self.conn

    
    def get_some_data(self):
        my_db = self.conn
        cur = my_db.cursor()

        sql = f"""select nationality, count(dictinct legal_entity_id) from {self.db_schema}.legalentity group by nationality;"""
        cur.execute(sql)
        df = pd.DataFrame(cur.fetchall())
        df.to_csv(r'my_data.csv')


if __name__ == '__main__':

    my_table1 = my_table()
    my_table1.get_connected()
    my_table1.get_some_data()
    my_db.close()

