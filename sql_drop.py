import mysql.connector
from mysql.connector import Error

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

def drop_database(connection, db_name):
    cursor = connection.cursor()
    cursor.execute(f"DROP DATABASE {db_name};")
    connection.commit()
    cursor.close()
    print(f"Database {db_name} dropped successfully.")

def drop_tables(connection, db_name):
    cursor = connection.cursor()
    cursor.execute(f"USE {db_name};")
    cursor.execute("SHOW TABLES;")
    tables = cursor.fetchall()
    for table in tables:
        cursor.execute(f"DROP TABLE {table[0]};")
    connection.commit()
    cursor.close()
    print(f"Tables in {db_name} dropped successfully.")


# Connection parameters
host = "localhost"
user = "root"  # Change to your MySQL username
password = ""  # Change to your MySQL password
db_name = ""  # Change to your database name

# Create a connection without specifying the database
conn = create_connection(host, user, password)

drop_database(conn, db_name)