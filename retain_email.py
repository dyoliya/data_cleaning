
import pandas as pd
import os

def clean_and_merge_files(directory):
    # Define columns to retain
    cols_to_retain = [
        "email"
    ]

    # Initialize empty list to store cleaned dataframes
    file_stats = []
    cleaned_dataframes = []
    total_rows_before = 0
    total_rows_after = 0

    # Process each file
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            filepath = os.path.join(directory, filename)
            df = pd.read_csv(filepath,low_memory=False)

            # Initial number of rows
            initial_rows = len(df)
            total_rows_before += initial_rows
            
            # Retain columns included in the cols_to_retain
            df = df[cols_to_retain]

            # Final number of rows
            final_rows = len(df)
            total_rows_after += final_rows
            
            # Store file statistics
            file_stats.append({
                'Filename': filename,
                'Rows Before': initial_rows,
                'Rows After': final_rows
            })

            # Store the cleaned dataframe
            cleaned_dataframes.append(df)

    # Merge all cleaned dataframes
    final_dataframe = pd.concat(cleaned_dataframes)

    # Final retained rows after merging
    final_rows_after_merge = len(final_dataframe)

    # Save into a single CSV file
    final_dataframe.to_csv('output/Rev.csv', index=False)
    
    # Save file statistics to an Excel file
    stats_df = pd.DataFrame(file_stats)
    stats_df.loc['Total After Merging'] = [
        'All Files Combined', 
        total_rows_before, 
        final_rows_after_merge
    ]
    stats_df.to_excel('output/Clean_Stats_Report1.xlsx', index=False)

# Print total statistics
    print(f'Total Rows Before Cleaning: {total_rows_before}')
    print(f'Total Rows After Cleaning: {total_rows_after}')
    print(f'Total Rows After Merging: {final_rows_after_merge}')
    
# Execution of script
clean_and_merge_files('for_processing')
