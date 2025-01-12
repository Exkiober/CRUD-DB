import mysql.connector
from mysql.connector import Error
import os

def create_database(connection, db_name):
    create_db_query = f"CREATE DATABASE IF NOT EXISTS {db_name}"
    execute_query(connection, create_db_query)

def create_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=user_password
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")
    return connection

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except Error as e:
        print(f"The error '{e}' occurred")

def execute_sql_files(connection, directory):
    for filename in os.listdir(directory):
        if filename.endswith('.sql'):
            file_path = os.path.join(directory, filename)
            with open(file_path, 'r', encoding='utf-8') as file:
                sql_commands = file.read()
                # Split commands by semicolon, but handle multi-line statements
                commands = sql_commands.split(';')
                try:
                    for command in commands:
                        command = command.strip()
                        if command:  # Avoid executing empty commands
                            execute_query(connection, command)
                    connection.commit()  # Commit all commands if successful
                    print(f"All commands from {filename} executed successfully.")
                except Error as e:
                    print(f"Error executing commands from {filename}. Rolling back.")
                    connection.rollback()  # Rollback on error
                    print(f"The error '{e}' occurred")

# Connection parameters
host = "localhost"
user = "root"  # Change to your MySQL username
password = ""  # Change to your MySQL password
db_name = ""  # Change to your database name

# Create a connection without specifying the database
conn = create_connection(host, user, password)

# Create the database if it doesn't exist
if conn is not None:
    create_database(conn, db_name)

    # Now connect to the new database
    conn.database = db_name

    # Directory containing SQL files
    sql_directory = os.path.join(os.getcwd(), 'sql_for_test')  # Change 'sql_for_test' to your directory name

    # Execute SQL files
    execute_sql_files(conn, sql_directory)

    # Close the connection
    if conn.is_connected():
        conn.close()
        print("MySQL connection is closed")
else:
    print("Failed to connect to the MySQL server.")