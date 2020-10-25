import mysql.connector                  #to be installed
from mysql.connector import Error


# Establishes and returns a connection to the database
def connect():
    try:
        conn = mysql.connector.connect(host="192.168.0.2", database="Covid-data", user="prova", password="prova")
        if conn.is_connected():
            print('Connected to MySQL database')
            return conn

    except Error as e:
        print(f"Error while connecting to the database: '{e}'")
        raise Exception
