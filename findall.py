import os
import shutil

# Define paths
source_root = r"C:\Users\danie\Documents\scripts\finalCode\2minTables"  # Tables folder path
destination_folder = r"C:\Users\danie\Documents\scripts\finalCode\groupedData"  # Destination folder

# Create the destination folder if it doesn't exist
os.makedirs(destination_folder, exist_ok=True)

# Iterate through each subfolder in the source root
for subfolder in os.listdir(source_root):
    subfolder_path = os.path.join(source_root, subfolder)

    # Check if it is a directory
    if os.path.isdir(subfolder_path):
        # Define a list to track the potential locations of the target file
        possible_files = []

        # Look in the output folder if it exists
        output_folder_path = os.path.join(subfolder_path, 'output')
        if os.path.exists(output_folder_path):
            possible_files.append(os.path.join(output_folder_path, 'grouped_unique_trips_with_timesGroupedTrips.csv'))

        # Also check directly in the subfolder itself
        possible_files.append(os.path.join(subfolder_path, 'grouped_unique_trips_with_timesGroupedTrips.csv'))

        # Iterate through the possible locations and copy if the file is found
        for target_file in possible_files:
            if os.path.exists(target_file):
                # Create a new name for the copied file
                new_file_name = f"{subfolder}_grouped_unique_trips_with_timesGroupedTrips.csv"
                destination_path = os.path.join(destination_folder, new_file_name)
                
                # Copy the file to the destination folder
                shutil.copy(target_file, destination_path)
                print(f"Copied: {target_file} to {destination_path}")
                break  # Stop after finding the file in one of the locations

print("All files copied successfully.")
