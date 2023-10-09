import os
import paramiko
import zipfile
import pandas as pd
import mysql.connector

# SFTP server details
sftp_host = 'your_sftp_server.com'
sftp_port = 22
sftp_username = 'your_sftp_username'
sftp_password = 'your_sftp_password'
sftp_remote_dir = '/path/to/remote/directory'

# MySQL database details
mysql_host = 'localhost'
mysql_port = 3306
mysql_username = 'your_mysql_username'
mysql_password = 'your_mysql_password'
mysql_database = 'your_mysql_database'

# Function to import CSV data into MySQL
def import_csv_to_mysql(csv_file_path):
    try:
        conn = mysql.connector.connect(
            host=mysql_host,
            port=mysql_port,
            user=mysql_username,
            password=mysql_password,
            database=mysql_database
        )
        cursor = conn.cursor()

        # Create a table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS your_table_name (
            column1 INT,
            column2 VARCHAR(255),
            -- Add more columns as needed
            PRIMARY KEY (column1)
        )
        """
        cursor.execute(create_table_query)

        # Load CSV data into MySQL
        load_data_query = f"""
        LOAD DATA LOCAL INFILE '{csv_file_path}'
        INTO TABLE your_table_name
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\\n'
        IGNORE 1 ROWS
        """
        cursor.execute(load_data_query)

        conn.commit()
        print(f"CSV data from '{csv_file_path}' imported into MySQL.")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()
        conn.close()

# Function to process uploaded files
def process_uploaded_file(file_name, sftp):
    local_file_path = os.path.join('.', file_name)
    
    # Download the file from SFTP
    sftp.get(os.path.join(sftp_remote_dir, file_name), local_file_path)
    
    # Check if it's a zip file
    if file_name.endswith('.zip'):
        with zipfile.ZipFile(local_file_path, 'r') as zip_ref:
            zip_ref.extractall('.')
        os.remove(local_file_path)  # Remove the zip file
        
        # Assuming the CSV file has the same name as the zip file
        csv_file_name = os.path.splitext(file_name)[0] + '.csv'
        csv_file_path = os.path.join('.', csv_file_name)
        
        # Import CSV data into MySQL
        import_csv_to_mysql(csv_file_path)
        os.remove(csv_file_path)  # Remove the CSV file

# Establish an SFTP connection
transport = paramiko.Transport((sftp_host, sftp_port))
transport.connect(username=sftp_username, password=sftp_password)
sftp = transport.open_sftp()

# List files in the SFTP directory
sftp.chdir(sftp_remote_dir)
files = sftp.listdir()

# Process newly updated files
for file_name in files:
    process_uploaded_file(file_name, sftp)

# Close the SFTP connection
sftp.close()
transport.close()
