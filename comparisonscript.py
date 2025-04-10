import os
import shutil

def compare_and_move_sp(folder1, folder2, output_folder):
    # Get the list of stored procedures in both folders
    sp_folder1 = set(os.listdir(folder1))
    sp_folder2 = set(os.listdir(folder2))

    # Find the extra stored procedures in folder1 that are not in folder2
    extra_sp = sp_folder1 - sp_folder2

    # Create the output folder if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Move the extra stored procedures to the output folder
    for sp in extra_sp:
        src_path = os.path.join(folder1, sp)
        dest_path = os.path.join(output_folder, sp)
        shutil.move(src_path, dest_path)
        print(f"Moved: {sp}")

    print(f"Total extra stored procedures moved: {len(extra_sp)}")

if __name__ == "__main__":
    # Hardcoded paths for the folders
    folder1 = r"D:\Sql-LinesSetup\sqlines-3.3.171\sqlines-3.3.171\all_sql"
    folder2 = r"D:\Sql-LinesSetup\sqlines-3.3.171\sqlines-3.3.171\Successs_Plus_Failure_SPS"
    output_folder = r"D:\Sql-LinesSetup\sqlines-3.3.171\sqlines-3.3.171\output_folder"

    # Call the function to compare and move stored procedures
    compare_and_move_sp(folder1, folder2, output_folder)