# Source: https://pynative.com/python-postgresql-tutorial/
import psycopg2
from psycopg2 import Error

connection = None

try:
   # Connect to an test database 
   # NOTE: 
   # 1. NEVER store credential like this in practice. This is only for testing purpose
   # 2. Replace your "database" name, "user" and "password" that we provide to test the connection to your database 
   db_params = {
       'host': 'dbcourse2022.cs.aalto.fi',
       'database': 'grp14_vaccinedist',
       'user': 'grp14',
       'password': '3c$H(08s',
       #'port': 5432
   }
   connection = psycopg2.connect(**db_params)


   # Create a cursor to perform database operations
   cursor = connection.cursor()
   # Print PostgreSQL details
   print("PostgreSQL server information")
   print(connection.get_dsn_parameters(), "\n")
   # Executing a SQL query
   cursor.execute("SELECT version();")
   # Fetch result
   record = cursor.fetchone()
   print("You are connected to - ", record, "\n")

except (Exception, Error) as error:
   print("Error while connecting to PostgreSQL", error)
finally:
   if (connection):
      cursor.close()
      connection.close()
      print("PostgreSQL connection is closed")