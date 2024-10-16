
import pandas as pd
import os

def clean_and_merge_files(directory):
    # Define columns to retain
    cols_to_retain = [
        "Keyword", "Name", "Full_Address", "City", "State", "Domain",
        "First_category", "Claimed_google_my_business", "Reviews_count", "Average_rating",
        "Coordinates", "Place_ID", "GMB_URL", "Google_Knowledge_URL", "Linkedin URL"
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
            
            # Delete columns included in the cols_to_delete
            df = df[cols_to_retain]

            # Strip whitespace from all relevant columns
            df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

            # Rename Place_ID to place_id and Domain to website
            df.rename(columns={'Place_ID': 'place_id'}, inplace=True)


            # Delete any rows where the `Coordinates`, `place_id`, and/or `Domain` column/s is/are empty
            df['Coordinates'] = df['Coordinates'].astype(str).str.strip()
            df['place_id'] = df['place_id'].astype(str).str.strip()
            df['Domain'] = df['Domain'].astype(str).str.strip()

            # Remove rows where Coordinates, place_id, or Domain are empty or contain only whitespace
            df = df[(df['Coordinates'] != '') & (df['place_id'] != '') & (df['Domain'] != '')]

            # Delete any row where the column Average_rating has a value of 5 or empty
            df = df[df['Average_rating'].astype(str).str.strip() != '']
            df = df[df['Average_rating'].astype(float) != 5]

            # Delete any row where the column Reviews_count has a value of 1, 0, or empty
            df = df[df['Reviews_count'].astype(str).str.strip() != '']
            df = df[df['Reviews_count'].astype(float) > 1]

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

    # Remove duplicate place_id and Domain across all files once merged
    final_dataframe.drop_duplicates(subset=['place_id'], inplace=True)
    final_dataframe.drop_duplicates(subset=['Domain'], inplace=True)
    final_dataframe.rename(columns={'Domain': 'website'}, inplace=True)
    # Final retained rows after merging
    final_rows_after_merge = len(final_dataframe)

    # Save into a single CSV file
    final_dataframe.to_csv('output/1 - Python Cleaned for DFS/Leads File.csv', index=False)
    
    # Save file statistics to an Excel file
    stats_df = pd.DataFrame(file_stats)
    stats_df.loc['Total After Merging'] = [
        'All Files Combined', 
        total_rows_before, 
        final_rows_after_merge
    ]
    stats_df.to_excel('output/1 - Python Cleaned for DFS/Clean_Stats_Report.xlsx', index=False)

# Print total statistics
    print(f'Total Rows Before Cleaning: {total_rows_before}')
    print(f'Total Rows After Cleaning: {total_rows_after}')
    print(f'Total Rows After Merging: {final_rows_after_merge}')
    
# Execution of script
clean_and_merge_files('for_processing')
