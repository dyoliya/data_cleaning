import os
import pandas as pd

# Directory where your CSV files are located
input_directory = 'for splitting'
output_directory = 'output/split_files'
max_rows = 20000

# Ensure the output directory exists
os.makedirs(output_directory, exist_ok=True)

# Process each CSV file in the directory
for filename in os.listdir(input_directory):
    if filename.endswith('.csv'):
        file_path = os.path.join(input_directory, filename)
        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path)
        
        # Get the total number of rows in the DataFrame
        num_rows = len(df)
        
        # Calculate the number of files we need to split into
        num_files = (num_rows // max_rows) + 1
        
        # Split the DataFrame into smaller DataFrames
        for i in range(num_files):
            start_row = i * max_rows
            end_row = min(start_row + max_rows, num_rows)
            
            # Create a smaller DataFrame
            df_split = df.iloc[start_row:end_row]
            
            # Write the smaller DataFrame to a new CSV file
            output_file_path = os.path.join(output_directory, f"{os.path.splitext(filename)[0]}_part_{i+1}.csv")
            df_split.to_csv(output_file_path, index=False)
            
            print(f'Saved {output_file_path}')

print("Splitting complete!")
