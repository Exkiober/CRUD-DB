from typing import Dict, List, Any
import json
from datetime import datetime
import os
import re


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

def generate_sql_file(data_cache: dict, output_dir: str = '', db_name: str = ''): 
    """Generate SQL INSERT statements from data cache."""
    os.makedirs(output_dir, exist_ok=True)  # Create output directory if it doesn't exist

    # Process each table in the data cache
    for table_name, table_info in data_cache.items():
        table_data = table_info['data']
        if not table_data:  # Skip if no data
            continue
        
        output_file = os.path.join(output_dir, f"{table_name}.sql")
        with open(output_file, 'w', encoding='utf-8') as f:
            # Write header
            f.write("-- SQL INSERT statements generated from cache\n")
            f.write("-- Generated at: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n\n")
            
            # Set SQL mode and character set
            f.write("SET SQL_MODE = 'NO_AUTO_VALUE_ON_ZERO';\n")
            f.write("SET NAMES utf8mb4;\n\n")
            
            # Select the database
            f.write(f"USE `{db_name}`;\n\n")
            
            # Write table header
            f.write(f"-- Data for table `{table_name}`\n")
            
            # Write INSERT statements
            for index, row in enumerate(table_data, start=1):  # Start ID from 1
                # Check if the row has valid data before writing
                if not any(row.values()):  # Skip empty rows
                    continue
                
                # Create the column names part
                columns = ', '.join(f'`{col}`' for col in row.keys())
                
                # Create the values part, handling different data types
                values = []
                for val in row.values():
                    if val is None:
                        values.append('NULL')
                    elif isinstance(val, (int, float)):
                        values.append(str(val))
                    elif isinstance(val, bool):
                        values.append('1' if val else '0')
                    else:
                        # Escape single quotes and wrap in quotes
                        val_str = str(val).replace("'", "''")  # Escape single quotes
                        values.append(f"'{val_str}'")
                
                # Set the ID to the current index (starting from 1)
                if 'id' in row:
                    values[0] = str(index)  # Assuming 'id' is the first column
                
                values_str = ', '.join(values)
                
                # Write the INSERT statement on a single line
                insert_sql = f"INSERT INTO `{table_name}` ({columns}) VALUES ({values_str});\n"
                f.write(insert_sql)
            
            f.write("\n")  # Add newline at the end of the file
        
        print(f"SQL INSERT statements generated successfully for table `{table_name}`: {output_file}")


# def generate_sql_file(data_cache: dict, output_file: str = '', db_name: str = ''):
#     """Generate SQL INSERT statements from data cache."""
#     with open(output_file, 'w', encoding='utf-8') as f:
#         # Write header
#         f.write("-- SQL INSERT statements generated from cache\n")
#         f.write("-- Generated at: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n\n")
        
#         # Set SQL mode and character set
#         f.write("SET SQL_MODE = 'NO_AUTO_VALUE_ON_ZERO';\n")
#         f.write("SET NAMES utf8mb4;\n\n")
        
#         # Select the database
#         f.write(f"USE `{db_name}`;\n\n")
        
#         # Process each table in the data cache
#         for table_name, table_info in data_cache.items():
#             table_data = table_info['data']
#             if not table_data:  # Skip if no data
#                 continue
            
#             # Write table header
#             f.write(f"-- Data for table `{table_name}`\n")
            
#             # Write INSERT statements
#             for index, row in enumerate(table_data, start=1):  # Start ID from 1
#                 # Check if the row has valid data before writing
#                 if not any(row.values()):  # Skip empty rows
#                     continue
                
#                 # Create the column names part
#                 columns = ', '.join(f'`{col}`' for col in row.keys())
                
#                 # Create the values part, handling different data types
#                 values = []
#                 for val in row.values():
#                     if val is None:
#                         values.append('NULL')
#                     elif isinstance(val, (int, float)):
#                         values.append(str(val))
#                     elif isinstance(val, bool):
#                         values.append('1' if val else '0')
#                     else:
#                         # Escape single quotes and wrap in quotes
#                         val_str = str(val).replace("'", "''")
#                         values.append(f"'{val_str}'")
                
#                 # Set the ID to the current index (starting from 1)
#                 if 'id' in row:
#                     values[0] = str(index)  # Assuming 'id' is the first column
                
#                 values_str = ', '.join(values)
                
#                 # Write the INSERT statement
#                 insert_sql = f"INSERT INTO `{table_name}` ({columns}) VALUES ({values_str});\n"
#                 f.write(insert_sql)
            
#             f.write("\n")  # Add newline between tables
        
#         print(f"SQL INSERT statements generated successfully: {output_file}")


def generate_create_table_sql(schema_file: str, db_name: str) -> List[str]:
    """Generate SQL CREATE TABLE statements from a JSON schema file."""
    create_statements = []

    # Load the schema from the JSON file
    try:
        with open(schema_file, 'r') as f:
            schema_cache = json.load(f)
    except FileNotFoundError:
        print(f"Schema file '{schema_file}' not found.")
        return []
    except json.JSONDecodeError:
        print(f"Error decoding JSON from schema file '{schema_file}'.")
        return []
    
    create_statements.append(f"CREATE DATABASE IF NOT EXISTS `{db_name}`;\n")
    create_statements.append(f"USE `{db_name}`;\n")

    # Process each table in the schema
    for table_name, table_info in schema_cache.items():
        columns_with_types = []
        
        for col, dtype in table_info['columns'].items():
            # Escape column names
            col = f"`{col}`"  # Escape column name
            
            # Replace AI with AUTO_INCREMENT and PK with PRIMARY KEY
            dtype = dtype.replace("AI", "AUTO_INCREMENT").replace("PK", "PRIMARY KEY")
            
            # Construct the column definition
            column_definition = f"{col} {dtype}"
            columns_with_types.append(column_definition)
        
        # Join the column definitions
        columns_with_types_str = ',\n    '.join(columns_with_types)  # Indent for readability
        
        # Create the CREATE TABLE statement
        create_table_sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` (\n    {columns_with_types_str}\n);\n"
        create_statements.append(create_table_sql)

    return create_statements


def write_sql_to_file(sql_statements: List[str], output_file: str) -> None:
    """Write SQL statements to a specified SQL file."""
    with open(output_file, 'w', encoding='utf-8') as f:
        for statement in sql_statements:
            # Escape single quotes in the SQL statement
            f.write(statement + "\n")  # Write each statement followed by a newline

if __name__ == "__main__":
    
    db_name = ''  # Replace with your actual database name
    
    # Get the current directory of the script
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Define the paths relative to the current directory
    schema_cache_path = os.path.join(current_dir, 'schema', 'latest_schema.json')
    # data_cache_path = os.path.join(current_dir, 'cache', 'updated_data_cache.json')
    
    # Load cached data
    schema_cache = load_cache_from_file(schema_cache_path)
    # data_cache = load_cache_from_file(data_cache_path)
    
    # Define the output directory
    output_dir = os.path.join(current_dir, 'sql')

    # Generate CREATE TABLE SQL
    create_table_sql = generate_create_table_sql(schema_cache_path, db_name)
    write_sql_to_file(create_table_sql, os.path.join(output_dir, 'create_table.sql'))

    # Generate the SQL file with INSERT statements
    # generate_sql_file(data_cache, output_dir)

    # generate_sql_file(data_cache, os.path.join(current_dir, 'sql_to_be_imported/staging/database_dump.sql'), db_name='db_name')