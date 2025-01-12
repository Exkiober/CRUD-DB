import json
import os
from datetime import datetime

# Define the base directory for the project
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # This can be set to a different path if needed

def load_schema(file_path):
    """Load schema from a JSON file."""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"File '{file_path}' not found.")
        return {}
    except json.JSONDecodeError:
        print(f"Error decoding JSON from file '{file_path}'.")
        return {}

def translate_column_definition(col_definition):
    """Translate column definitions to SQL syntax."""
    # Replace AI with AUTO_INCREMENT and PK with PRIMARY KEY
    col_definition = col_definition.replace("AI", "AUTO_INCREMENT").replace("PK", "PRIMARY KEY")
    return col_definition

def compare_and_generate_statements(latest_schema, old_schema, db_name):
    """Compare schemas and generate ALTER TABLE and CREATE TABLE statements."""
    alter_statements = []
    create_statements = []

    # Check for missing columns in existing tables
    for table_name, old_info in old_schema.items():
        if table_name not in latest_schema:
            print(f"Table '{table_name}' does not exist in the latest schema.")
            continue
        
        old_columns = old_schema[table_name]['columns']
        latest_columns = latest_schema[table_name]['columns']

        for col_name, col_definition in latest_columns.items():
            if col_name not in old_columns:
                # Translate column definition
                translated_definition = translate_column_definition(col_definition)
                # Generate ALTER TABLE statement
                alter_statement = f"ALTER TABLE `{table_name}` ADD COLUMN `{col_name}` {translated_definition};"
                alter_statements.append(alter_statement)
                print(f"Missing column '{col_name}' in table '{table_name}'. Generated: {alter_statement}")

    # Check for missing tables in the old schema
    for table_name, latest_info in latest_schema.items():
        if table_name not in old_schema:
            # Generate CREATE TABLE statement
            columns_with_types = []
            for col_name, col_definition in latest_info['columns'].items():
                translated_definition = translate_column_definition(col_definition)
                columns_with_types.append(f"`{col_name}` {translated_definition}")
            columns_with_types_str = ',\n    '.join(columns_with_types)  # Indent for readability
            create_table_sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` (\n    {columns_with_types_str}\n);\n"
            create_statements.append(create_table_sql)
            print(f"Table '{table_name}' is missing. Generated: {create_table_sql}")

    return alter_statements, create_statements

def main():
    local_schema_file = os.path.join(BASE_DIR, 'schema/latest_schema.json') 
    staging_schema_file = os.path.join(BASE_DIR, 'schema/main_staging_schema_cache.json')
    db_name = ''  # Replace with your actual database name

    local_schema = load_schema(local_schema_file)
    staging_schema = load_schema(staging_schema_file)

    alter_statements, create_statements = compare_and_generate_statements(local_schema, staging_schema, db_name)

    # Get current date and time for the filename
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(BASE_DIR, f'sql/migration/{current_time}_alter_create_statements.sql')

    # Ensure the migration directory exists; create it if it doesn't
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Write the ALTER and CREATE statements to a file
    with open(output_file, 'w', encoding='utf-8') as f:
        for statement in alter_statements:
            f.write(statement + '\n')
        for statement in create_statements:
            f.write(statement + '\n')

    print(f"ALTER and CREATE TABLE statements written to '{output_file}'.")

if __name__ == "__main__":
    main()