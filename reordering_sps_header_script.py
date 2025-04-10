import re
import os

def process_sql_file(input_file_path, output_file_path):
    """Process a single SQL file: extract, reorder procedure header, and combine with body."""
    try:
        with open(input_file_path, 'r', encoding='utf-8') as file:
            procedure_code = file.read()
    except UnicodeDecodeError:
        print(f"Failed to read file due to encoding issues: {input_file_path}")
        return

    # Regex to extract the procedure header
    header_pattern = r"CREATE OR REPLACE PROCEDURE\s+\w+\s*\(.*?\)\s*AS"

    # Search for the procedure header
    match = re.search(header_pattern, procedure_code, re.DOTALL | re.IGNORECASE)

    if match:
        # Extract the header and remaining body
        procedure_header = match.group(0)
        procedure_body = procedure_code[match.end():].strip()

        # Reorder parameters within the extracted header
        parameter_pattern = re.compile(r"\s*(\w+)\s+(\w+\s*(?:\([^\)]*\))?)(\s*=\s*\w+)?\s*(,|\))")
        lines = procedure_header.strip().split("\n")
        header_first_line = lines[0].strip()
        parameters_block = "\n".join(lines[1:]).strip()

        # Find all parameter matches
        parameters = parameter_pattern.findall(parameters_block)

        # Classify parameters into default and non-default
        default_params = []
        non_default_params = []

        for param_name, param_type, default, _ in parameters:
            param = f"{param_name} {param_type.strip()}"
            if default:
                default_params.append(f"{param}{default.strip()}")
            else:
                non_default_params.append(param)

        # Combine reordered parameters: non-default first, then default
        ordered_params = non_default_params + default_params
        formatted_params = ",\n    ".join(ordered_params)

        # Generate the final reordered procedure header
        reordered_header = f"""{header_first_line}
            
    {formatted_params}
)"""


       # Combine reordered header with procedure body
        final_procedure = f"{reordered_header}\nAS\n{procedure_body}"

        # Write the modified SQL to the output file
        with open(output_file_path, 'w', encoding='utf-8') as file:
            file.write(final_procedure)

        print(f"Processed file: {input_file_path} -> {output_file_path}")
    else:
        print(f"Procedure header not found in file: {input_file_path}")


def process_sql_files_in_folder(input_folder_path, output_folder_path):
    """Process all .sql files in the input folder and save them to the output folder."""
    if not os.path.isdir(input_folder_path):
        print(f"Invalid input folder path: {input_folder_path}")
        return

    if not os.path.exists(output_folder_path):
        os.makedirs(output_folder_path)

    # Iterate over all files in the input folder
    for file_name in os.listdir(input_folder_path):
        if file_name.endswith('.sql'):
            input_file_path = os.path.join(input_folder_path, file_name)
            output_file_path = os.path.join(output_folder_path, file_name)
            process_sql_file(input_file_path, output_file_path)


# Input and output folder paths
input_folder_path = r"D:\Sql-LinesSetup\sqlines-3.3.171\sqlines-3.3.171\Failed_Sps_Without_reordering"
output_folder_path = r"D:\Sql-LinesSetup\sqlines-3.3.171\sqlines-3.3.171\Ordered_Failed_Sps"

# Process SQL files
process_sql_files_in_folder(input_folder_path, output_folder_path)
