import os
import pandas as pd

def process_csv_files(input_folder, output_file):
    # Create lists to hold file data and names
    data_frames = []
    filenames = []

    # Loop through each CSV file in the specified folder
    for file in os.listdir(input_folder):
        if file.endswith('.csv'):
            file_path = os.path.join(input_folder, file)
            df = pd.read_csv(file_path)

            # Ensure that the column names are as expected
            if 'Unique_Locations' in df.columns and 'Count' in df.columns:
                # Rename 'Count' column to distinguish between files
                count_column_name = f"{file}_count"
                df = df[['Unique_Locations', 'Count']].copy()
                df.rename(columns={'Count': count_column_name}, inplace=True)

                # Add filename to the list for later use
                filenames.append(file)

                # Append the dataframe
                data_frames.append(df)

    # Merge all dataframes on 'Unique_Locations'
    if not data_frames:
        print("No CSV files found with the required columns.")
        return
    
    combined_df = data_frames[0]
    for df in data_frames[1:]:
        combined_df = pd.merge(combined_df, df, on='Unique_Locations', how='outer')

    # Replace NaN values with 0 for counts
    combined_df.fillna(0, inplace=True)

    # Add percentage columns
    for file in filenames:
        count_column = f"{file}_count"
        percent_column = f"{file}_percent"
        combined_df[percent_column] = (combined_df[count_column] / combined_df[count_column].sum()) * 100

    # Save the resulting dataframe to a new CSV file
    combined_df.to_csv(output_file, index=False)

    print(f"Processed file saved to: {output_file}")

# Specify the input folder and output file path
input_folder = r"C:\Users\danie\Documents\scripts\finalCode\groupedData"  # Update with the path to your folder
output_file = r"C:\Users\danie\Documents\scripts\finalCode\name.csv"  # Update with the desired output file path

# Run the function
process_csv_files(input_folder, output_file)
