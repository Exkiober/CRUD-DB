import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def remove_encryption_flag(folder_path: str):
    """Scan SQL files in the specified folder and remove ENCRYPTION='Y'."""
    # Iterate over all files in the specified folder
    for filename in os.listdir(folder_path):
        if filename.endswith('.sql'):  # Check if the file is an SQL file
            file_path = os.path.join(folder_path, filename)
            with open(file_path, 'r', encoding='utf-8') as file:
                content = file.read()  # Read the content of the file
            
            # Remove occurrences of ENCRYPTION='Y'
            updated_content = content.replace("ENCRYPTION='Y'", "")
            
            # Write the updated content back to the file
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(updated_content)
            
            print(f"Updated file: {filename}")

# Example usage
remove_encryption_flag(os.path.join(BASE_DIR, 'path/to/your/sql_directory'))