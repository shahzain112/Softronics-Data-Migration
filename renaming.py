import os
import re

def rename_sql_files(folder_path):
    for filename in os.listdir(folder_path):
        if filename.endswith(".sql"):
            new_filename = re.sub(r"_converted\.sql$", ".sql", filename)
            old_path = os.path.join(folder_path, filename)
            new_path = os.path.join(folder_path, new_filename)
            
            if filename != new_filename:
                os.rename(old_path, new_path)
                print(f"Renamed: {filename} -> {new_filename}")

if __name__ == "__main__":
    folder_path = r'D:\Sql-LinesSetup\sqlines-3.3.171\sqlines-3.3.171\DestinationFolderForDump'
    if os.path.isdir(folder_path):
        rename_sql_files(folder_path)
    else:
        print("Invalid folder path.")
