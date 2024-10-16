import pandas as pd
import os

def clean_and_process_file(directory):
    # Define columns to retain
    cols_to_retain = [
        "Keyword", "Name", "Full_Address", "City", "State", "website", "First_category",
        "Claimed_google_my_business", "Reviews_count", "Average_rating", "Coordinates", "place_id",
        "GMB_URL", "Linkedin URL", "negative_review", "count_less_than_three"
    ]

    # Initialize variables to track row counts
    total_rows_before = 0
    total_rows_after = 0

    # Look for the first CSV file in the directory
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            filepath = os.path.join(directory, filename)
            df = pd.read_csv(filepath, low_memory=False)

            # Initial number of rows
            initial_rows = len(df)
            total_rows_before += initial_rows

            # Keep only the specified columns
            df = df[cols_to_retain]

            # Strip whitespace from all relevant columns
            df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

            # Rename columns for consistency
            df.rename(columns={'Reviews_count': 'total_review_count'}, inplace=True)
            df.rename(columns={'count_less_than_three': 'count_reviews_1-3 stars'}, inplace=True)
            df.rename(columns={'Average_rating': 'Rating'}, inplace=True)

            # Clean up the 'website' column and remove rows with empty or whitespace-only 'website'
            df['website'] = df['website'].astype(str).str.strip()
            df = df[df['website'] != '']

            # Remove duplicate 'website' values
            df.drop_duplicates(subset=['website'], inplace=True)

            # Final number of rows
            final_rows = len(df)
            total_rows_after += final_rows

            # Extract the file name without the extension
            file_name_without_ext = os.path.splitext(filename)[0]

            # Create the output file path with the desired format
            output_path = os.path.join('output/2 - Cleaned for Clay', f'Cleaned_for_Clay_{file_name_without_ext}.csv')

            # Save the cleaned dataframe to a new CSV file
            df.to_csv(output_path, index=False)
            
            # Exit after processing the first CSV file
            break

    # Print total statistics
    print(f'Processed file: {filename}')
    print(f'Total Rows Before Cleaning: {total_rows_before}')
    print(f'Total Rows After Cleaning: {total_rows_after}')

# Execution of script
clean_and_process_file('for_processing')
