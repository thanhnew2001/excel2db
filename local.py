import os
import time
import pandas as pd
import zipfile
import paramiko
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Function to perform the conversion and upload for a single Excel file
def process_excel_file(excel_file_path, sftp):
    csv_file_path = excel_file_path.replace(".xlsx", ".csv")
    zip_file_path = csv_file_path.replace(".csv", ".zip")

    # Convert Excel to CSV
    df = pd.read_excel(excel_file_path)
    df.to_csv(csv_file_path, index=False)

    # Zip the CSV file
    with zipfile.ZipFile(zip_file_path, 'w') as zipf:
        zipf.write(csv_file_path, os.path.basename(csv_file_path))

    # Upload the zipped file to SFTP server
    sftp.put(zip_file_path, os.path.basename(zip_file_path))

    # Clean up: remove temporary files
    os.remove(csv_file_path)
    os.remove(zip_file_path)

    print(f"Excel file '{excel_file_path}' converted to CSV, zipped, and uploaded to SFTP server.")

# Function to handle new file events
class NewFileHandler(FileSystemEventHandler):
    def __init__(self, sftp):
        self.sftp = sftp

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith(".xlsx"):
            process_excel_file(event.src_path, self.sftp)

# Define your SFTP server details
sftp_host = 'your_sftp_server.com'
sftp_port = 22
sftp_username = 'your_username'
sftp_password = 'your_password'
sftp_remote_dir = '/path/to/remote/directory'

# Create an SFTP connection
transport = paramiko.Transport((sftp_host, sftp_port))
transport.connect(username=sftp_username, password=sftp_password)
sftp = transport.open_sftp()

# Set up the folder monitoring
folder_to_watch = '/path/to/your/excel/files'
event_handler = NewFileHandler(sftp)
observer = Observer()
observer.schedule(event_handler, path=folder_to_watch, recursive=False)
observer.start()

try:
    print(f"Monitoring folder '{folder_to_watch}' for new Excel files. Press Ctrl+C to stop...")
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    observer.stop()

observer.join()
sftp.close()
transport.close()
