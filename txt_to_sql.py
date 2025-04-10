import os
import shutil

def convert_txt_to_sql(source_folder, destination_folder):
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    for file in os.listdir(source_folder):
        source_path = os.path.join(source_folder, file)
        
        if file.endswith('.txt'):
            new_file_name = file.replace('.txt', '.sql')
            destination_path = os.path.join(destination_folder, new_file_name)

            shutil.copy(source_path, destination_path)
            print(f"Converted: {file} -> {new_file_name}")

        elif file.endswith('.sql'):
            destination_path = os.path.join(destination_folder, file)
            shutil.copy(source_path, destination_path)
            print(f"Copied: {file}")

if __name__ == "__main__":
    source_folder = r'D:\Sql-LinesSetup\sqlines-3.3.171\sqlines-3.3.171\all_sql\FailedSps'
    destination_folder = r'D:\Sql-LinesSetup\sqlines-3.3.171\sqlines-3.3.171\all_sql\FailedSps\Failed_sps_sql'

    if os.path.exists(source_folder) and os.path.isdir(source_folder):
        convert_txt_to_sql(source_folder, destination_folder)
    else:
        print("Invalid source folder path!")
