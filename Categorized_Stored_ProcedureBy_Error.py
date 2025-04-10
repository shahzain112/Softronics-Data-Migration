import os
import shutil

# Define paths for the input and output folders
stored_procedures_folder = r"D:\Sql-LinesSetup\sqlines-3.3.171\sqlines-3.3.171\Failure_Ordered_Sps"  
log_file_path = r"E:\_All_Python_Scripts_ForData_Migration_For_SQl_Server_\410_Sps_log_File.txt"  
output_folder_path = r"D:\Sql-LinesSetup\sqlines-3.3.171\sqlines-3.3.171\MyCategory_Error_Wise_Stored_Procedures_Folder"

# Ensure the output folder exists, if not, create it
if not os.path.exists(output_folder_path):
    os.makedirs(output_folder_path)

# Function to read the log file and categorize stored procedures
def categorize_stored_procedures(log_file_path):
    error_categories = {}

    # Check if the log file exists
    if not os.path.exists(log_file_path):
        print(f"Error: The log file '{log_file_path}' does not exist.")
        return error_categories

    # Read the log file
    with open(log_file_path, 'r') as file:
        for line in file:
            # Assuming each line in the log is in the format: stored_procedure_name, error_category
            # Modify this line parsing as per the actual format of the log file
            parts = line.strip().split(',')  # Split by comma or modify delimiter as needed
            if len(parts) == 2:
                stored_proc, error_category = parts
                if error_category not in error_categories:
                    error_categories[error_category] = []
                error_categories[error_category].append(stored_proc)

    return error_categories

# Function to move stored procedures into corresponding folders
def move_stored_procedures(error_categories, stored_procedures_folder, output_folder_path):
    # Loop through each error category
    for error_category, stored_procs in error_categories.items():
        # Create a folder for the error category
        error_folder_path = os.path.join(output_folder_path, error_category)
        if not os.path.exists(error_folder_path):
            os.makedirs(error_folder_path)

        # Loop through each stored procedure
        for stored_proc in stored_procs:
            stored_proc_file = stored_proc + '.sql'  # Assuming stored procedures are SQL files
            stored_proc_path = os.path.join(stored_procedures_folder, stored_proc_file)

            # Check if the file exists in the stored procedures folder
            if os.path.exists(stored_proc_path):
                # Move the stored procedure to the error folder
                shutil.move(stored_proc_path, os.path.join(error_folder_path, stored_proc_file))
            else:
                print(f"Stored procedure {stored_proc_file} not found in the stored procedures folder.")

# Main execution
def main():
    # Step 1: Categorize stored procedures based on the log file
    error_categories = categorize_stored_procedures(log_file_path)

    # Step 2: Move the stored procedures into the corresponding error folders if categories exist
    if error_categories:
        move_stored_procedures(error_categories, stored_procedures_folder, output_folder_path)
        print("Stored procedures have been organized successfully.")
    else:
        print("No error categories found or log file issue.")

# Run the script
if __name__ == "__main__":
    main()
