import pandas as pd
import os

def clean_and_process_file(directory):
    # Define columns to retain
    cols_to_retain = [
        "Keyword", "Name", "City", "website", "First_category", "place_id", "GMB_URL", "Rating",
        "count_reviews_1-3 stars", "Full Name", "Final Email", "firstName", "clean_customer_name",
        "review_star_rating", "gmb_categories_singular", "gmb_categories_plural", "clean_business_name",
        "total_review_count", "variant_1_I_found", "Coordinates"
    ]

    # Initialize variables to track row counts
    total_rows_before = 0
    total_rows_after = 0
    total_invalid_rating_rows = 0 

    # Look for the first CSV file in the directory
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            filepath = os.path.join(directory, filename)
            df = pd.read_csv(filepath, low_memory=False)

            # Initial number of rows
            initial_rows = len(df)
            total_rows_before += initial_rows

            # Strip whitespace from all relevant columns
            df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

            # Delete any rows where the `Coordinates`, `place_id`, and/or `Domain` column/s is/are empty
            df['Cleaned firstName'] = df['Cleaned firstName'].astype(str).str.strip()
            df['Negative Review Selection'] = df['Negative Review Selection'].astype(str).str.strip()
            df['Cleaned Customer Name'] = df['Cleaned Customer Name'].astype(str).str.strip()
            df['review_star_rating'] = df['review_star_rating'].astype(str).str.strip()
            df['Clean_business_name'] = df['Clean_business_name'].astype(str).str.strip()
            df['gmb_categories_singular'] = df['gmb_categories_singular'].astype(str).str.strip()
            df['gmb_categories_plural'] = df['gmb_categories_plural'].astype(str).str.strip()
            df['variant_1_I_found'] = df['variant_1_I_found'].astype(str).str.strip()

            # Keep only the specified columns
            df = df[cols_to_retain]

            # Rename columns for consistency
            df.rename(columns={'Final Email': 'email'}, inplace=True)

            # Clean and process the 'review_star_rating' column
            # Convert all values to strings and remove non-digit rows
            df['review_star_rating'] = df['review_star_rating'].astype(str).str.strip()

            # Convert the 'review_star_rating' to numeric, forcing invalid values to NaN
            df['review_star_rating'] = pd.to_numeric(df['review_star_rating'], errors='coerce')

            # Remove rows where review_star_rating is NaN or outside the range [1, 5]
            invalid_rows_mask = df['review_star_rating'].isna() | ~df['review_star_rating'].between(1, 5)
            invalid_rating_rows = df[invalid_rows_mask]
            total_invalid_rating_rows += len(invalid_rating_rows)

            # Remove invalid rows
            df = df[~invalid_rows_mask]

            # Convert the 'review_star_rating' column to integer type to avoid float-like values (1.0, 2.0, etc.)
            df['review_star_rating'] = df['review_star_rating'].astype(int)

            # Final number of rows
            final_rows = len(df)
            total_rows_after += final_rows

            # Extract the file name without the extension
            file_name_without_ext = os.path.splitext(filename)[0]

            # Create the output file path with the desired format
            output_path = os.path.join('output/3 - Cleaned for Instantly', f'Cleaned_{file_name_without_ext}.csv')

            # Save the cleaned dataframe to a new CSV file
            df.to_csv(output_path, index=False)

            # Exit after processing the first CSV file
            break

    # Print total statistics
    print(f'Processed file: {filename}')
    print(f'Total Rows Before Cleaning: {total_rows_before}')
    print(f'Total Rows with Invalid Ratings Removed: {total_invalid_rating_rows}')
    print(f'Total Rows After Cleaning: {total_rows_after}')


# Execution of script
clean_and_process_file('for_processing')
