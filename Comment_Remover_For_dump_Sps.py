import os

# Define input and output directories
input_dir = r"D:\Sql-LinesSetup\sqlines-3.3.171\sqlines-3.3.171\DestinationFolderForDump"
output_dir = r"D:\Sql-LinesSetup\sqlines-3.3.171\sqlines-3.3.171\RemovedCommentSPS"

# Ensure the output directory exists
os.makedirs(output_dir, exist_ok=True)

# Comment to remove
comment_to_remove = "-- SQLINES FOR EVALUATION USE ONLY (14 DAYS)"

# Iterate through all files in the input directory
for filename in os.listdir(input_dir):
    # Only process files with a .sql extension
    if filename.endswith(".sql"):
        input_path = os.path.join(input_dir, filename)
        output_path = os.path.join(output_dir, filename)
        
        # Read the input file
        with open(input_path, "r", encoding="utf-8") as file:
            content = file.read()
        
        # Remove the comment
        updated_content = content.replace(comment_to_remove, "").strip()
        
        # Write the updated content to the output file
        with open(output_path, "w", encoding="utf-8") as file:
            file.write(updated_content)

print("All stored procedures have been processed and saved to the output directory.")
