import os
import shutil

# Define directories and files
categorized_error_dir = r"D:\Sql-LinesSetup\sqlines-3.3.171\sqlines-3.3.171\Categorized_Error_Folder"
failure_ordered_file = r"D:\Sql-LinesSetup\sqlines-3.3.171\sqlines-3.3.171\Failure_Ordered_Sps"
output_folder = r"D:\Sql-LinesSetup\sqlines-3.3.171\sqlines-3.3.171\Extra_Sps_Filtered_Out_From_410"

# Create output folder if it doesn't exist
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Gather all stored procedures from the 4 categorized folders
categorized_sps = set()
for root, _, files in os.walk(categorized_error_dir):
    for file in files:
        if file.endswith(".sql"):  # Assuming stored procedures are .sql files
            categorized_sps.add(file)

# Read all stored procedures from the Failure_Ordered_Sps file
failure_ordered_sps = set()
with open(failure_ordered_file, "r") as f:
    for line in f:
        sp = line.strip()  # Remove whitespace and newline
        if sp:
            failure_ordered_sps.add(sp)

# Find extra stored procedures in the 410 list
extra_sps = failure_ordered_sps - categorized_sps

# Move extra stored procedures to the output folder
for sp in extra_sps:
    # Assume the .sql files for 410 are in the same directory as the Failure_Ordered_Sps file
    sp_path = os.path.join(os.path.dirname(failure_ordered_file), sp)
    if os.path.exists(sp_path):  # Check if the SP file exists
        shutil.move(sp_path, os.path.join(output_folder, sp))
    else:
        print(f"File not found: {sp_path}")

# Print results
print(f"Total stored procedures in 410 list: {len(failure_ordered_sps)}")
print(f"Stored procedures in categorized folders: {len(categorized_sps)}")
print(f"Extra stored procedures found and moved: {len(extra_sps)}")
print(f"Extra stored procedures are saved in: {output_folder}")
