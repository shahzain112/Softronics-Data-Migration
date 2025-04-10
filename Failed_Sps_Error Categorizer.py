import os
import re
from pathlib import Path
import shutil  # For copying files

def categorize_errors(log_file_path, input_folder_path, output_folder_path):
    # Read the log file
    with open(log_file_path, 'r') as log_file:
        logs = log_file.readlines()

    # Dictionary to store errors and associated files
    error_categories = {}
    unmatched_logs = []
    input_files = {file.name for file in Path(input_folder_path).iterdir() if file.is_file()}

    # Regular expression to extract file names and error types
    pattern = r"^(.*?_converted\.sql): (.+?) at or near .+"

    # Parse the log file
    for log in logs:
        print(f"Processing log: {log.strip()}")
        match = re.match(pattern, log)
        if match:
            file_name = match.group(1)
            error_type = match.group(2)
            print(f"Matched file: {file_name}, Error: {error_type}")

            # Categorize by error type
            if error_type not in error_categories:
                error_categories[error_type] = []
            error_categories[error_type].append(file_name)
        else:
            unmatched_logs.append(log.strip())

    # Ensure output directory exists
    output_folder = Path(output_folder_path)
    output_folder.mkdir(parents=True, exist_ok=True)

    # Create folders and organize files
    categorized_files = set()
    for error_type, files in error_categories.items():
        # Create a sanitized folder name
        folder_name = re.sub(r"[^\w\s-]", "_", error_type[:50])
        error_folder = output_folder / folder_name
        error_folder.mkdir(exist_ok=True)
        print(f"Creating folder: {error_folder}")

        for file_name in files:
            source_path = Path(input_folder_path) / file_name
            print(f"Checking source path: {source_path}")
            if source_path.exists():
                # Create a symbolic link (or copy if needed)
                link_path = error_folder / file_name
                if not link_path.exists():
                    shutil.copy(source_path, link_path)
                    categorized_files.add(file_name)
                    print(f"Copied {source_path} to {link_path}")
            else:
                print(f"Source file not found: {file_name}")

    # Print debugging information
    print(f"Total SPS in input folder: {len(input_files)}")
    print(f"Total SPS in log file: {len(logs)}")
    print(f"Total categorized SPS: {len(categorized_files)}")
    print(f"Unmatched log lines: {len(unmatched_logs)}")
    for unmatched in unmatched_logs:
        print(f"Unmatched: {unmatched}")

    # Check for missing files
    missing_files = input_files - categorized_files
    if missing_files:
        print(f"Files in input folder but not categorized: {len(missing_files)}")
        for missing in missing_files:
            print(f"Missing: {missing}")

    print(f"Categorized files are saved in '{output_folder_path}'.")

# Usage
log_file = r"E:\PythonScriptsForMigration\For_527_Ordered_SPS_log_file.txt"
input_folder = r"D:\Sql-LinesSetup\sqlines-3.3.171\sqlines-3.3.171\Failure_Ordered_Sps"
output_folder = r"D:\Sql-LinesSetup\sqlines-3.3.171\sqlines-3.3.171\Categorized_Error_Folder"
categorize_errors(log_file, input_folder, output_folder)
