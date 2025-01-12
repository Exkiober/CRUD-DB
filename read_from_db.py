import mysql.connector
from typing import Dict, List, Any
import json
from datetime import datetime
import os

def create_connection(host, user, password, database):
    """Establish a database connection."""
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None


def close_connection(connection):
    """Close the database connection."""
    if connection.is_connected():
        connection.close()
        print("Database connection closed.")

def retrieve_data(connection):
    """Retrieve data from all tables in the connected database."""
    try:
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()

        for (table_name,) in tables:
            print(f"Data from table: {table_name}")
            cursor.execute(f"SELECT * FROM {table_name}")
            rows = cursor.fetchall()

            # Get column names
            column_names = [i[0] for i in cursor.description]
            print(f"{column_names}")

            # Print each row of data in the order of column names
            for row in rows:
                ordered_row = [row[col] for col in column_names]  # Ensure order matches column_names
                print(ordered_row)
            print("\n")  # Add a newline for better readability

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()

def get_column_types(cursor, table_name: str) -> Dict[str, str]:
    """Get column names and their types for a table, including lengths for VARCHAR, default values, NOT NULL, AI, and Pri."""
    cursor.execute(f"""
        SELECT COLUMN_NAME, 
               DATA_TYPE, 
               COLUMN_DEFAULT, 
               IS_NULLABLE, 
               COLUMN_KEY, 
               EXTRA, 
               CHARACTER_MAXIMUM_LENGTH 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{table_name}'
        AND TABLE_SCHEMA = DATABASE()  -- Ensure we are querying the correct database
        ORDER BY ORDINAL_POSITION
    """)
    
    column_types = {}
    for col_name, data_type, column_default, is_nullable, column_key, extra, char_length in cursor.fetchall():
        # Construct the data type string
        if char_length is not None:
            data_type = f"{data_type}({char_length})"
        
        # Add NOT NULL information
        if is_nullable == 'NO':
            data_type += " NOT NULL"
        
        # Add default value information
        if column_default is not None:
            data_type += f" DEFAULT '{column_default}'"
        
        # Add primary key and auto-increment information
        if column_key == 'PRI':
            data_type += " PRIMARY KEY"  # Change to PRIMARY KEY
        if 'auto_increment' in extra:
            data_type += " AUTO_INCREMENT"  # Change to AUTO_INCREMENT
        
        column_types[col_name] = data_type
    
    return column_types

def cache_database_schema(connection) -> Dict[str, Dict]:
    """Cache the database schema including table names, column names, and types."""
    schema_cache = {}
    try:
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()

        for (table_name,) in tables:
            # Get column types for each table in the correct order
            column_types = get_column_types(cursor, table_name)
            
            # Update mediumtext(16777215) to text(65535)
            for col_name, data_type in column_types.items():
                if data_type == "mediumtext(16777215)":
                    column_types[col_name] = "text(65535)"  # Change to text(65535)

            schema_cache[table_name] = {
                "columns": column_types,
                "last_updated": datetime.now().isoformat()
            }

    except mysql.connector.Error as err:
        print(f"Error caching schema: {err}")
    finally:
        cursor.close()
    
    return schema_cache

def cache_table_data(connection, schema_cache: Dict) -> Dict[str, List[Dict]]:
    """Cache the data from all tables."""
    data_cache = {}
    try:
        cursor = connection.cursor(dictionary=True)  # Return results as dictionaries
        
        for table_name in schema_cache.keys():
            print(f"Caching data from table: {table_name}")
            # Get the column names in the correct order
            column_names = list(schema_cache[table_name]['columns'].keys())
            # Create a SELECT statement with the columns in the desired order
            column_list = ', '.join(column_names)
            cursor.execute(f"SELECT {column_list} FROM {table_name}")
            data_cache[table_name] = {
                "data": cursor.fetchall(),
                "last_updated": datetime.now().isoformat()
            }

    except mysql.connector.Error as err:
        print(f"Error caching data: {err}")
    finally:
        cursor.close()
    
    return data_cache


def save_cache_to_file(cache: Dict, filename: str):
    """Save the cache to a JSON file."""
    try:
        # Convert datetime objects to strings for JSON serialization
        with open(filename, 'w') as f:
            json.dump(cache, f, indent=2, default=str)
        print(f"Cache saved to {filename}")
    except Exception as e:
        print(f"Error saving cache: {e}")

def load_cache_from_file(filename: str) -> Dict:
    """Load the cache from a JSON file."""
    try:
        with open(filename, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Cache file {filename} not found")
        return {}
    except Exception as e:
        print(f"Error loading cache: {e}")
        return {}

def remove_unwanted_columns(schema_cache: Dict, data_cache: Dict, columns_to_remove: List[str]) -> None:
    """Remove specified columns from schema and data caches."""
    # Remove specified columns from schema_cache
    for table_info in schema_cache.values():
        for col in columns_to_remove:
            if col in table_info['columns']:
                del table_info['columns'][col]

    # Remove specified columns from data_cache
    for table_name, table_info in data_cache.items():
        for item in table_info['data']:
            for col in columns_to_remove:
                if col in item:
                    del item[col]

def remove_tables_from_cache(schema_cache: Dict, data_cache: Dict, tables_to_remove: List[str]) -> None:
    """Remove specified tables from the data cache and schema cache."""
    
    # Print initial state of caches
    print("Initial Data Cache:", data_cache)
    print("Initial Schema Cache:", schema_cache)

    # Remove tables from data cache and schema cache
    for table in tables_to_remove:
        # Remove from data cache
        if table in data_cache:
            del data_cache[table]  # Remove the table from the data cache
            print(f"Removed table '{table}' from data cache.")
        else:
            print(f"Table '{table}' not found in data cache.")

        # Remove from schema cache
        if table in schema_cache:
            del schema_cache[table]  # Remove the table from the schema cache
            print(f"Removed table '{table}' from schema cache.")
        else:
            print(f"Table '{table}' not found in schema cache.")



if __name__ == "__main__":
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))  # Add this line

    host = "localhost"  # Database host
    user = "root"  # Database username
    password = ""  # Database password
    database = ""  # Database name

    # Create a database connection
    db_connection = create_connection(host, user, password, database)

    if db_connection:
        # Cache schema and data
        schema_cache = cache_database_schema(db_connection)
        data_cache = cache_table_data(db_connection, schema_cache)

        
        # # Define columns to be removed
        # columns_to_remove = [
        #     'Host', 'User', 'Select_priv', 'Insert_priv', 'Update_priv', 
        #     'Delete_priv', 'Create_priv', 'Drop_priv', 'Reload_priv', 
        #     'Shutdown_priv', 'Process_priv', 'File_priv', 'Grant_priv', 
        #     'References_priv', 'Index_priv', 'Alter_priv', 'Show_db_priv', 
        #     'Super_priv', 'Create_tmp_table_priv', 'Lock_tables_priv', 
        #     'Execute_priv', 'Repl_slave_priv', 'Repl_client_priv', 
        #     'Create_view_priv', 'Show_view_priv', 'Create_routine_priv', 
        #     'Alter_routine_priv', 'Create_user_priv', 'Event_priv', 
        #     'Trigger_priv', 'Create_tablespace_priv', 'ssl_type', 
        #     'ssl_cipher', 'x509_issuer', 'x509_subject', 'max_questions', 
        #     'max_updates', 'max_connections', 'max_user_connections', 
        #     'plugin', 'authentication_string', 'password_expired', 
        #     'password_last_changed', 'password_lifetime', 'account_locked', 
        #     'Create_role_priv', 'Drop_role_priv', 'Password_reuse_history', 
        #     'Password_reuse_time', 'Password_require_current', 'User_attributes'
        # ]

        # # Specify the tables to remove
        # tables_to_remove = ["_prisma_migrations", "_OwnerOrganization"]

        # # Remove unwanted columns from caches
        # remove_unwanted_columns(schema_cache, data_cache, columns_to_remove)

        # # Call the function to remove the specified tables
        # remove_tables_from_cache(schema_cache, data_cache, tables_to_remove)


        # Save caches to files using the script directory
        save_cache_to_file(schema_cache, os.path.join(script_dir, 'schema/latest_schema.json'))  # Update this line
        # save_cache_to_file(data_cache, os.path.join(script_dir, 'cache/local_data_cache.json'))  # Update this line

        # Example of loading cache
        # loaded_schema = load_cache_from_file(os.path.join(script_dir, 'schema/latest_schema.json'))  # Update this line
        # loaded_data = load_cache_from_file(os.path.join(script_dir, 'cache/local_data_cache.json'))  # Update this line
       
        # Close the database connection
        close_connection(db_connection)

        # Example of using the cached data
        # for table_name, table_info in loaded_schema.items():
        #     print(f"\nTable: {table_name}")
        #     print("Columns and types:", table_info['columns'])
        #     print("Last updated:", table_info['last_updated'])
            # if table_name in loaded_data:
            #     print(f"Number of rows: {len(loaded_data[table_name]['data'])}")