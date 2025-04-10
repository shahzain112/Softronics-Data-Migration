import os
import shutil

# Define the paths
txt_file_path = r'D:\Sql-LinesSetup\sqlines-3.3.171\sqlines-3.3.171\different_sps_names\different_sps_names.txt'
source_folder_path = r'D:\Sql-LinesSetup\sqlines-3.3.171\sqlines-3.3.171\DestinationFolderForDump'
output_folder_path = r'D:\Sql-LinesSetup\sqlines-3.3.171\sqlines-3.3.171\different_sps_names\output_folder'

# Ensure the output folder exists
if not os.path.exists(output_folder_path):
    os.makedirs(output_folder_path)

# Read the stored procedure names from the text file
with open(txt_file_path, 'r') as file:
    sp_names = [line.strip() for line in file.readlines()]

# Iterate over the stored procedures in the source folder
for sp_name in sp_names:
    sp_file_name = f"{sp_name}.sql"
    sp_file_path = os.path.join(source_folder_path, sp_file_name)
    
    # Check if the file exists in the source folder
    if os.path.exists(sp_file_path):
        # Move the file to the output folder
        shutil.move(sp_file_path, os.path.join(output_folder_path, sp_file_name))
        print(f"Moved: {sp_file_name}")
    else:
        print(f"File not found: {sp_file_name}")

print("Operation completed.")