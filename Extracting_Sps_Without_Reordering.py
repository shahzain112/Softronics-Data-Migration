import os
import shutil

# Define directory paths
failed_sps_dir = r"D:\Sql-LinesSetup\sqlines-3.3.171\sqlines-3.3.171\Failed_Sps_Without_reordering"
ordered_failed_sps_dir = r"D:\Sql-LinesSetup\sqlines-3.3.171\sqlines-3.3.171\Ordered_Failed_Sps"
extra_sps_dir = r"D:\Sql-LinesSetup\sqlines-3.3.171\sqlines-3.3.171\Extra_Without_Order_Sps"

# Ensure the extra directory exists
os.makedirs(extra_sps_dir, exist_ok=True)

# Get the list of files in both directories
failed_sps_files = set(os.listdir(failed_sps_dir))
ordered_sps_files = set(os.listdir(ordered_failed_sps_dir))

# Find the extra files in the failed_sps_dir
extra_files = failed_sps_files - ordered_sps_files

# Copy the extra files to the new directory
for file_name in extra_files:
    src_path = os.path.join(failed_sps_dir, file_name)
    dest_path = os.path.join(extra_sps_dir, file_name)
    shutil.copy(src_path, dest_path)

print(f"Extra files saved to: {extra_sps_dir}")
