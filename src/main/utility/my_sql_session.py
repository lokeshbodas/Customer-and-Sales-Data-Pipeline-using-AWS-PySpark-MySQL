import os, sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "C:\VS Code\Customer-and-Sales-Data-Pipeline-using-AWS-PySpark-MySQL-main"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import mysql.connector
from resources.dev import config
def get_mysql_connection():
    connection = mysql.connector.connect(
        host=config.db_host,
        user=config.db_user,
        password=config.db_password,
        database=config.db_name
    )
    return connection

















# connection = mysql.connector.connect(
#     host="localhost",
#     user="root",
#     password="password",
#     database="manish"
# )
#
# # Check if the connection is successful
# if connection.is_connected():
#     print("Connected to MySQL database")
#
# cursor = connection.cursor()
#
# # Execute a SQL query
# query = "SELECT * FROM manish.testing"
# cursor.execute(query)
#
# # Fetch and print the results
# for row in cursor.fetchall():
#     print(row)
#
# # Close the cursor
# cursor.close()
#
# connection.close()
