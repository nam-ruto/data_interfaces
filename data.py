import pandas as pd
import pymysql
import pyodbc

class DataInterface:
    def __init__(self, db_type, connection_params):
        self.db_type = db_type
        self.connection_params = connection_params
        self.connect = None
    
    def connect_database(self):
        try:
            if self.db_type == "csv":
                self.connect = None
            elif self.db_type == "MySQL":
                self.connect = pymysql.connect(**self.connection_params) # ** operator is used to unpacked the dictionary 
            elif self.db_type == "SQL Sever":
                self.connect = pyodbc.connect(self.connection_params)
            else:
                raise ValueError("Unsupported database type")
            if self.connect:
                print("Connected to " + self.db_type)
        except Exception as e:
            print(e)

    def read(self, query_or_path):
        if self.db_type in ["MySQL", "SQL Sever"]:
            cursor = self.connect.cursor()
            cursor.execute(query_or_path)
            rows = cursor.fetchall() # This will return a list of tuple, each tuple is a row
            rows = [tuple(row) for row in rows]
            df = pd.DataFrame(rows, columns=["id", "name", "gender", "age"])
            return df, rows
        else:
            return pd.read_csv(query_or_path)
        
    def create_table(self, table_name, columns):
        cursor = self.connect.cursor()
        formatted_columns = [f"`{col}`" for col in columns]
        query = f"CREATE TABLE {table_name} ({', '.join(columns)})"
        print("Executing query:", query)
        cursor.execute(query)
        self.connect.commit()

    def write_to_mysql(self, data, table_name):
        cursor = self.connect.cursor()
        query = f"INSERT INTO {table_name} VALUES (%s, %s, %s, %s)"
        cursor.executemany(query, data)
        self.connect.commit()    

    def close(self):
        if self.connect:
            self.connect.close()

    
def csvFile(filePath):
    database = DataInterface("csv", None)
    print(database.read(filePath))

def MySQL(mysql_params):
    database = DataInterface("MySQL", mysql_params)
    database.connect_database()

    df, data = database.read("SELECT * FROM student")
    print(df) # Print dataframe into the console screen

    table_name = "student_copy_2"
    columns = ["id INT PRIMARY KEY", "name VARCHAR(50)", "gender VARCHAR(20)", "age INT"]
    database.create_table(table_name, columns)
    database.write_to_mysql(data, table_name)

def sql_sever(sql_sever_paramas):
    database = DataInterface("SQL Sever", sql_sever_paramas)
    database.connect_database()
    print(database.read("SELECT * FROM student"))



if __name__ == "__main__":
    # CSV
    # csvFile("my_data.csv")

    # MySQL
    mysql_params = {
        'host': 'localhost',
        'user': 'root',
        'password': '1234',
        'db': 'my_database'
    }
    MySQL(mysql_params)

    # SQL Sever
    connection_string = (
    r"DRIVER={SQL Server};"
    r"SERVER=DESKTOP-I5TALT9\NAMRUTO;"
    r"DATABASE=my_database"
    )
    # sql_sever(connection_string)
