import pandas as pd

def add_custom_column_with_range(file_path, output_path, new_column_name, num_locations_range, specific_location):
    # Load the CSV file
    df = pd.read_csv(file_path)

    # Function to check if Unique_Locations has the specific number of locations within the range and contains the specific location
    def check_conditions(row):
        # Convert the string to a tuple
        locations = eval(row['Unique_Locations'])
        return num_locations_range[0] <= len(locations) <= num_locations_range[1] and specific_location in locations

    # Add the new column based on the conditions
    df[new_column_name] = df.apply(check_conditions, axis=1)

    # Reorder columns to make the new column the first one
    columns = [new_column_name] + [col for col in df.columns if col != new_column_name]
    df = df[columns]

    # Save the updated DataFrame to a new CSV file
    df.to_csv(output_path, index=False)

    print(f"New column '{new_column_name}' added and file saved to {output_path}.")


# Example usage:
file_path = 'name_updated(1).csv'
output_path = 'name_updated(2).csv'
new_column_name = 'Contains_Apgar_in_3_to_4'
num_locations_range = (3, 4)  # Specify the range (min, max)
specific_location = 'Apgar'

add_custom_column_with_range(file_path, output_path, new_column_name, num_locations_range, specific_location)

