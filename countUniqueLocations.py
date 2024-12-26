import pandas as pd

# Load the CSV file
file_path = r'C:\Users\danie\Documents\scripts\finalCode\name.csv'  # Replace with your actual file path
df = pd.read_csv(file_path)

# Extract the number of unique locations from 'Unique_Locations' and create a new column for it
df['Number of Locations'] = df['Unique_Locations'].apply(lambda x: len(eval(x)) if isinstance(x, str) else 0)

# Group by the number of locations and sum the other count columns
grouped_df = df.groupby('Number of Locations').sum(numeric_only=True).reset_index()

# Save the result to a new CSV file
output_file_path = r'C:\Users\danie\Documents\scripts\finalCode\countUnique.csv'  # Replace with your desired output file path
grouped_df.to_csv(output_file_path, index=False)

print(f"Summary CSV file has been saved to {output_file_path}")
