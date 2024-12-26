# Imports
import pandas as pd
import re
import csv
import os
from datetime import datetime
from collections import defaultdict

# Larger Locations and their associated locations
location_filters = {
    "Apgar": ["Apgar_village", "Apgar_campground", "Rocky_point_trail", "Apgar_VC", "Apgar_VC_parking"],

    "Avalanche": ["Avalanche_trail", "Trail_of_Cedars", "Avalanche_parking", "Shuttle_Avalanche"],

    "Logan": ["Logan_pass_VC",  "Hidden_lake_trail", "Loop_highline_grinnell_glac", "Oberlin_climbing", 
              "Logan_pass_parking", "Hidden_lake_overlook", "Shuttle_Logan_pass"],

    "LakeMcDonald": ["Shuttle_Lake_macdonald", "Lake_macdonald_lodge"],

    "Loop": ["Loop_parking","Shuttle_Loop"],

    "Waterfalls": ["Gunsight_trailhead_stMary_falls", "Baring_falls_parking", "St_mary_falls_parking", "StMary_falls_trail", 
                   "Baring_falls_trail", "Baring_falls_to_st_mary_falls", "Baring_falls_to_sun_point",
                   "Viriginia_falls_trail", "Shuttle_St_mary_falls", "Jackson_glac_overlook", "Shuttle_Jackson_glac_overlook", 
                   "Shuttle_Sunrift_gorge"],

    "SunPoint": ["Sun_point_nature_trail", "Sun_point_parking", "Shuttle_Sun_point"],

    "RisingSun": ["Rising_sun_boat_dock",  "Rising_sun_campground", "Rising_sun_picnic_area", "Rising_sun", 
                  "Shuttle_Rising_Sun", "Otokomi_lake_trail"],

    "StMary": ["St_mary_campground", "St_mary_VC", "St_mary_VC_parking"],

    "Not Sorted": ["Fish_creek", "Moose_country" , "Wild_goose_island", "Oberlin_bend", "Siyeh_bend_trail", "Big_bend", 
            "Lunch_creek", "Siyeh_pass_trail","Red_eagle_trail","Red_rock", "Siyeh_bend_parking", 
            "Lunch_creek_parking", "Shuttle_Siyeh_bend", "Many_glacier_trail"]

}
# Functions

def remove_invalid_locations_and_fuse(csv_input, csv_output):
    
    #Check if Csv exist
    if not os.path.exists(csv_input):
        print(f"File not found: {csv_input}")
        return

    # Load the CSV file
    df = pd.read_csv(csv_input)
    
    # Iterate over each row to remove "Not Sorted" or "Unknown" locations and fuse consecutive locations
    for index, row in df.iterrows():
        # Keep track of valid fused values
        fused_data = []
        
        for i in range(1, len(row) // 4 + 1):  # Looping through location_1, location_2, etc.
            location = row[f'location_{i}']
            
            # Skip invalid locations
            if location in ['Not Sorted', 'Unknown']:
                continue
            
            # Handle datetime conversion and possible NaT values for missing data
            enter_time = pd.to_datetime(row[f'enter_time_{i}'], errors='coerce')
            exit_time = pd.to_datetime(row[f'exit_time_{i}'], errors='coerce')
            
            # Handle missing duration values
            try:
                duration = float(row[f'duration_{i}']) if pd.notna(row[f'duration_{i}']) else 0
            except ValueError:
                duration = 0
            
            # If location is the same as the previous fused location, fuse them
            if len(fused_data) > 0 and location == fused_data[-1][0]:
                fused_data[-1][2] = max(fused_data[-1][2], exit_time)  # Update exit time
                fused_data[-1][3] += duration  # Sum duration
            else:
                # Append a new block of valid data
                fused_data.append([location, enter_time, exit_time, duration])
        
        # Fill in the row with fused valid data and shift remaining data leftward
        for i, (location, enter_time, exit_time, duration) in enumerate(fused_data, start=1):
            df.at[index, f'location_{i}'] = location
            df.at[index, f'enter_time_{i}'] = enter_time if pd.notna(enter_time) else None
            df.at[index, f'exit_time_{i}'] = exit_time if pd.notna(exit_time) else None
            df.at[index, f'duration_{i}'] = duration if duration != 0 else None
        
        # Clear the remaining columns beyond the fused data
        for j in range(len(fused_data) + 1, len(row) // 4 + 1):
            df.at[index, f'location_{j}'] = None
            df.at[index, f'enter_time_{j}'] = None
            df.at[index, f'exit_time_{j}'] = None
            df.at[index, f'duration_{j}'] = None

    # Save the updated DataFrame to a new CSV file
    df.to_csv(csv_output, index=False)


def analyze_trips(file_path, output_order_file, output_unique_file, output_grouped_order_file, output_grouped_unique_file):
    if not os.path.exists(file_path):
        print(f"File not found: {csv_input}")
        return    
    # Read the CSV file
    df = pd.read_csv(file_path)

    # Lists for individual trip data and dictionaries for grouped trip data
    order_trip_data = []
    unique_trip_data = []
    trip_orders = {}
    unique_locations = {}

    for idx, row in df.iterrows():
        id = row['id']
        locations = []
        total_duration = 0
        trip_start_time = None
        trip_end_time = None

        # Process location columns and compute total duration, start and end time
        for i in range(1, len(row) // 5 + 1):  # Assuming each set of location columns is (location, enter_time, exit_time, duration)
            loc = row[f'location_{i}']
            if pd.notna(loc):
                locations.append(str(loc))
                duration = row[f'duration_{i}']
                total_duration += duration if pd.notna(duration) else 0

                enter_time = pd.to_datetime(row[f'enter_time_{i}'], errors='coerce')
                exit_time = pd.to_datetime(row[f'exit_time_{i}'], errors='coerce')

                # Update trip start and end times
                if enter_time is not pd.NaT and (trip_start_time is None or enter_time < trip_start_time):
                    trip_start_time = enter_time
                if exit_time is not pd.NaT and (trip_end_time is None or exit_time > trip_end_time):
                    trip_end_time = exit_time

        # Format start and end times as strings without 'Timestamp'
        formatted_start_time = trip_start_time.strftime('%Y-%m-%d %H:%M:%S') if trip_start_time else None
        formatted_end_time = trip_end_time.strftime('%Y-%m-%d %H:%M:%S') if trip_end_time else None

        # Calculate total trip duration
        trip_duration = (trip_end_time - trip_start_time).total_seconds() / 60 if trip_start_time and trip_end_time else None

        # Add each ID and associated trip data to order_trip_data
        order_trip_data.append({
            'ID': id,
            'Trip_Order': tuple(locations),
            'Total_Duration_POI': total_duration,
            'Trip_Start_Time': formatted_start_time,
            'Trip_End_Time': formatted_end_time,
            'Total_Trip_Duration': trip_duration
        })

        # Add each ID and associated unique location trip data to unique_trip_data
        unique_trip_data.append({
            'ID': id,
            'Unique_Locations': tuple(sorted(set(locations))),
            'Total_Duration_POI': total_duration,
            'Trip_Start_Time': formatted_start_time,
            'Trip_End_Time': formatted_end_time,
            'Total_Trip_Duration': trip_duration
        })

        # Group data for trip order
        loc_tuple = tuple(locations)
        if loc_tuple not in trip_orders:
            trip_orders[loc_tuple] = {'IDs': [], 'Total_Duration_POI': [], 'Trip_Start_Time': [], 'Trip_End_Time': [], 'Total_Trip_Duration': []}
        trip_orders[loc_tuple]['IDs'].append(id)
        trip_orders[loc_tuple]['Total_Duration_POI'].append(total_duration)
        trip_orders[loc_tuple]['Trip_Start_Time'].append(formatted_start_time)
        trip_orders[loc_tuple]['Trip_End_Time'].append(formatted_end_time)
        trip_orders[loc_tuple]['Total_Trip_Duration'].append(trip_duration)

        # Group data for unique locations
        unique_set = tuple(sorted(set(locations)))
        if unique_set not in unique_locations:
            unique_locations[unique_set] = {'IDs': [], 'Total_Duration_POI': [], 'Trip_Start_Time': [], 'Trip_End_Time': [], 'Total_Trip_Duration': []}
        unique_locations[unique_set]['IDs'].append(id)
        unique_locations[unique_set]['Total_Duration_POI'].append(total_duration)
        unique_locations[unique_set]['Trip_Start_Time'].append(formatted_start_time)
        unique_locations[unique_set]['Trip_End_Time'].append(formatted_end_time)
        unique_locations[unique_set]['Total_Trip_Duration'].append(trip_duration)

    # Convert individual trip data lists to DataFrames
    order_df = pd.DataFrame(order_trip_data)
    unique_df = pd.DataFrame(unique_trip_data)

    # Convert grouped trip data to DataFrames
    grouped_order_df = pd.DataFrame([(k, v['IDs'], len(v['IDs']), v['Total_Duration_POI'], v['Trip_Start_Time'], v['Trip_End_Time'], v['Total_Trip_Duration'])
                                     for k, v in trip_orders.items()],
                                    columns=['Trip_Order', 'IDs', 'Count', 'Total_Duration_POI', 'Trip_Start_Time', 'Trip_End_Time', 'Total_Trip_Duration'])

    grouped_unique_df = pd.DataFrame([(k, v['IDs'], len(v['IDs']), v['Total_Duration_POI'], v['Trip_Start_Time'], v['Trip_End_Time'], v['Total_Trip_Duration'])
                                      for k, v in unique_locations.items()],
                                     columns=['Unique_Locations', 'IDs', 'Count', 'Total_Duration_POI', 'Trip_Start_Time', 'Trip_End_Time', 'Total_Trip_Duration'])

    # Save the individual trip data to CSV files
    order_df.to_csv(output_order_file, index=False)
    unique_df.to_csv(output_unique_file, index=False)

    # Save the grouped trip data to CSV files
    grouped_order_df.to_csv(output_grouped_order_file, index=False)
    grouped_unique_df.to_csv(output_grouped_unique_file, index=False)

    return order_df, unique_df, grouped_order_df, grouped_unique_df

def parse_logs(log_file_path, target_ids):
    log_entries = []
    current_id = None
    all_skipped_ids = set()

    with open(log_file_path, 'r') as file:
        for line in file:
            # Skip irrelevant lines
            if 'Processing shuttle stop polygons' in line or 'Tracker reached the end of data.' in line:
                continue
            
            # Extract ID
            if 'Processing group_id' in line:
                current_id = re.search(r'\d+', line)
                if current_id:
                    current_id = current_id.group()
            # Only process if the current ID is in the target list
            if current_id and current_id not in target_ids:
                continue

            # Extract entry and exit times for both POI and Shuttle Stops
            if 'entered polygon' in line:
                location_match = re.search(r"polygon '(.*?)'", line)
                enter_time_match = re.search(r'at (.*?) \(', line)
                if location_match and enter_time_match:
                    location = location_match.group(1)
                    enter_time = enter_time_match.group(1)
            elif 'exited polygon' in line:
                exit_time_match = re.search(r'at (.*?) \(', line)
                if current_id and location and enter_time and exit_time_match:
                    exit_time = exit_time_match.group(1)
                    # Calculate duration
                    enter_time_dt = pd.to_datetime(enter_time)
                    exit_time_dt = pd.to_datetime(exit_time)
                    duration = (exit_time_dt - enter_time_dt).total_seconds()  # Duration in seconds

                    if duration >= 120:  # Only add valid entries with sufficient duration
                        log_entries.append({
                            'id': current_id,
                            'location': location,
                            'enter_time': enter_time,
                            'exit_time': exit_time,
                            'duration': duration
                        })
                    else:
                        all_skipped_ids.add(current_id)

    # Identify IDs that were completely skipped
    processed_ids = {entry['id'] for entry in log_entries}
    completely_skipped_ids = all_skipped_ids - processed_ids

    # Print all skipped IDs due to short durations
    if completely_skipped_ids:
        print("Skipped IDs due to all short durations:", completely_skipped_ids)

    return log_entries

def create_csv(log_entries, output_file_path, duration_threshold):
    # Filter log entries based on duration threshold
    filtered_entries = [entry for entry in log_entries if entry['duration'] >= duration_threshold]

    # Create a DataFrame
    df = pd.DataFrame(filtered_entries)

    # Format the 'id' column as text to preserve leading zeros
    df['id'] = df['id'].apply(lambda x: f"'{x}")  # Adding a leading apostrophe

    # Convert enter_time to datetime for sorting
    df['enter_time'] = pd.to_datetime(df['enter_time'], errors='coerce')
    
    # Sort by enter_time
    df = df.sort_values(by=['id', 'enter_time']).reset_index(drop=True)
    
    # Create a new DataFrame for the final output
    max_locations = df.groupby('id').size().max()  # Get the maximum number of locations for any ID
    result_data = []

    for id_, group in df.groupby('id'):
        # Prepare the data row for the current ID
        row = {'id': id_}
        for i in range(len(group)):
            row[f'location_{i + 1}'] = group.iloc[i]['location']
            row[f'enter_time_{i + 1}'] = group.iloc[i]['enter_time']
            row[f'exit_time_{i + 1}'] = group.iloc[i]['exit_time']
            row[f'duration_{i + 1}'] = group.iloc[i]['duration']
        result_data.append(row)

    # Create a new DataFrame from the result data
    result_df = pd.DataFrame(result_data)
    
    # Save to CSV
    result_df.to_csv(output_file_path, index=False)



def map_locations_to_groups(input_file_path, output_file_path):
    # Read the input CSV file
    df = pd.read_csv(input_file_path)

    # Function to map location to group, keeping blanks as blanks
    def map_location_filters(location):
        if pd.isna(location) or location == "":  # Check for blanks or NaN
            return ""
        for group, locations in location_filters.items():
            if location in locations:
                return group
        return "Unknown"  # Return "Unknown" if location doesn't match any group

    # Loop through the columns and apply the mapping only to the location columns
    location_columns = [col for col in df.columns if col.startswith('location_')]

    for loc_col in location_columns:
        # Apply the mapping function to the location columns
        df[loc_col] = df[loc_col].apply(map_location_filters)

    # Save the modified DataFrame to the output CSV file
    df.to_csv(output_file_path, index=False)

    print(f"New group-based CSV file saved to: {output_file_path}")


# Function to map location to group (with the provided filters)
def map_location_filters(location, location_filters):
    for group, locations in location_filters.items():
        if location in locations:
            return group
    return None

# Function to process the CSV and generate separate files for each group
def process_grouped_locations_to_separate_files(input_file, output_folder, location_filters):
    # Load the CSV file into a DataFrame
    df = pd.read_csv(input_file)

    # Create output folder if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Iterate over each group in location_filters
    for group, group_locations in location_filters.items():
        result_rows = []
        group_ids = []  # To store IDs for each group

        # Iterate over each row (each unique ID)
        for index, row in df.iterrows():
            id_value = row['id']
            grouped_locations = []
            last_group = None

            # Iterate over the location columns (assuming columns are location_1, enter_time_1, exit_time_1, duration_1, etc.)
            col_idx = 1
            while f'location_{col_idx}' in row and not pd.isna(row[f'location_{col_idx}']):
                location = row[f'location_{col_idx}']
                group_match = map_location_filters(location, location_filters)

                # Only include locations that match the current group
                if group_match == group:
                    grouped_locations.append({
                        'location': location,
                        'enter_time': row.get(f'enter_time_{col_idx}'),
                        'exit_time': row.get(f'exit_time_{col_idx}'),
                        'duration': row.get(f'duration_{col_idx}')
                    })
                col_idx += 1

            # Only process if there are valid grouped locations
            if grouped_locations:
                result_row = {'id': id_value}
                for idx, loc_data in enumerate(grouped_locations):
                    loc_idx = idx + 1  # Start index at 1 for location_1, enter_time_1, ...
                    result_row[f'location_{loc_idx}'] = loc_data['location']
                    result_row[f'enter_time_{loc_idx}'] = loc_data['enter_time']
                    result_row[f'exit_time_{loc_idx}'] = loc_data['exit_time']
                    result_row[f'duration_{loc_idx}'] = loc_data['duration']
                result_rows.append(result_row)
                group_ids.append(id_value)  # Record the ID for the group

        # Convert the result rows into a DataFrame
        if result_rows:  # Only save if there are rows for the group
            result_df = pd.DataFrame(result_rows)
            output_file = os.path.join(output_folder, f'{group}.csv')
            result_df.to_csv(output_file, index=False)
            print(f'Saved {group} data to {output_file}')

import os

# Main Execution
if __name__ == "__main__":
    # Get the current working directory
    current_directory = os.getcwd()

    # Specify the folder name to create in the current directory
    base_folder = os.path.join(current_directory, "ApgarOnly" + "2min")

    # Create the base folder if it doesn't exist
    os.makedirs(base_folder, exist_ok=True)

    # Create input and output subfolders within the base folder
    output_folder = base_folder

    # Ensure the input and output folders exist
    os.makedirs(output_folder, exist_ok=True)

    # Define file paths using the input and output folders
    log_file_path = "DoubleFilterLogs.txt"
    output_file_path = os.path.join(output_folder, "formatted_log_data.csv")



    duration_threshold = 120

    target_ids = ['12521', '14151', '21081', '27341', '14541', '22531', '05361', '06351', '23011', '14461', '06171', '07191', '11421', '01401', '1512', '05521', '25171', '04051', '02501', '09501', '02341', '22151', '02481', '09421', '07341', '1551', '07261', '03471', '23171', '16261', '11411', '01371', '18271', '20181', '21191', '17501', '17331', '09191', '06451', '16421', '07221', '14181', '03551', '02241', '17141', '17021', '02541', '02291', '02211', '09341', '05251', '24441', '25161', '15011', '15191', '02451', '11531', '08231', '07171', '15421', '19331', '05331', '15441', '14111', '07181', '16021', '20071', '02031', '19041', '19521', '02091', '20081', '21381', '13231', '13381', '02351', '07031', '25331', '02081', '16461', '12011', '24231', '25321', '03411', '20021', '23421', '05351', '06461', '17231', '22261', '17161', '16181', '14161', '03031', '07301', '07531', '16101', '15101', '17111', '15261', '14411', '12021', '02521', '02011', '10091', '10231', '01341', '05081', '16171', '02281', '14271', '02301', '05261', '19541', '19011', '02271', '09081', '14431', '10331', '05461', '14441', '17321', '16531', '24481', '16121', '02471', '25431', '07391', '12161', '23541', '07441', '07121', '10081', '17381', '23201', '22091', '12031', '10031', '17391', '16441', '20431', '16501', '25281', '09161', '19021', '07451', '01501', '25071', '07161', '07351', '23281', '02251', '07461', '13511', '18471', '07521', '05171', '20241', '14521', '07541', '25241', '25011', '14091', '02141', '20331', '12401', '05311', '07131', '15461', '09071', '25201', '12301', '23041', '23511', '20391', '17541', '10241', '06181', '11261', '17431', '14531', '17471', '15161', '10391', '21521', '25421', '06511', '25291', '11331', '21501', '02551', '14321', '05221', '22121', '12501', '07321', '05131', '16081', '05541', '06531', '05451', '13241', '05091', '06291', '09401', '06151', '16341', '05061', '02201', '05401', '09351', '17131', '16291', '17461', '22021', '10531', '23341', '15532', '07211', '14011', '05211', '03041', '23291', '20161', '02101', '23441', '14281', '06441', '06021', '16521', '04301', '09281', '06541', '05511', '14191', '15501', '02191', '16111', '25041', '07251', '13331', '25231', '02051', '06261', '11111', '02411', '20221', '22221', '06421', '22271', '05071', '06521', '03131', '18151', '22411', '24041', '05041', '09201', '11381', '16311', '01031', '01061', '02161', '24321', '22471', '06041', '05341', '14081', '16271', '09041', '19461', '05441', '20291', '15321', '06111', '13041', '09291', '16091', '02511', '06481', '17531', '02421', '18131', '23071', '07481', '16161', '06271', '01511', '16321', '07361', '02181', '02021', '15251', '07071', '09331', '16221', '23521', '25141', '12251', '23231', '15291', '07231', '05291', '20041', '06061', '25391', '16351', '02331', '02221', '24541', '1321', '21341', '09381', '16411', '17271', '17441', '05011', '17341', '25311', '19311', '07041', '02061', '10471', '05121', '21141', '07091', '17511', '06131', '02431', '24341', '14221', '05151', '02321', '10361', '03311', '07411', '02111', '09471', '15091', '22401', '05231', '20201', '12151', '03161', '05141', '22461', '15271', '17121', '21231', '17081', '0355', '23101', '24171', '07101', '19241', '16011', '06301', '06241', '07081', '20411', '02441', '17041', '15411', '19161', '14421', '02361', '02171', '1313', '01241', '02531', '09151', '17221', '06381', '09111', '14171', '07271', '16471', '19181', '03201', '21351', '02231', '20381', '25181', '23141', '14101', '12091', '02461', '03191', '03101']

    bad_ids = ['10021', '10131', '10351', '10011', '10121', '20461', '23321', '09171', '10111', '10221', '10551', '20011', '20011', '02401', '02401', '10431', '10541', '14501', '17071', '20321', '06391', '10301', '10521', '07371', '10511', '25081', '25081', '07471', '07471', '14291', '14291', '15281', '17151', '24081', '02071', '05371', '09441', '12081', '16371', '16481', '16481', '07011', '18121', '02381', '09311', '14481', '15471', '24381', '25371', '25371', '02151', '02261', '07431', '14141', '15021', '21171', '10161', '14121', '15111', '24241', '10371', '17521', '18401', '23351', '25111', '03321', '10141', '10251', '11241', '11461', '17401', '20261']



    # Process logs and create CSV
    log_entries = parse_logs(log_file_path, [element for element in target_ids if element not in bad_ids])
    create_csv(log_entries, output_file_path, duration_threshold)

    # Process_grouped_locations_to_separate_files
    input_file = output_file_path
    process_grouped_locations_to_separate_files(input_file, output_folder, location_filters)

    # Function to create directories and file paths for grouped locations
    def create_file_paths(base_folder, location_filters):
        # Ensure the base folder exists, create if it doesn't
        if not os.path.exists(base_folder):
            os.makedirs(base_folder)

        # Dictionary to store file paths
        file_paths = {}

        # Loop through the dictionary to create directories and file paths
        for key in location_filters:
            # Create the file path for each key
            file_path = os.path.join(base_folder, f"{key}.csv")
        
            # Add the file path to the dictionary
            file_paths[key] = file_path

        return file_paths

    # Create the file paths for grouped locations
    file_paths = create_file_paths(output_folder, location_filters)

    # Loop through each file in file_paths
    for key, path in file_paths.items():
        # Define the input and cleaned output paths
   
        csv_input = path
        csv_output = os.path.join(output_folder, f"grouped_Log_data_{key}.csv")

        if not os.path.exists(path):
            print(f"File not found: {path}")
            continue 
        
        # Clean the file before further processing
        remove_invalid_locations_and_fuse(csv_input, csv_output)

        # Create unique output file paths by appending the key
        output_order_file = os.path.join(output_folder, f'individual_order_trips_with_times_{key}.csv')
        output_unique_file = os.path.join(output_folder, f'individual_unique_trips_with_time_{key}.csv')
        output_grouped_order_file = os.path.join(output_folder, f'grouped_order_trips_with_times_{key}.csv')
        output_grouped_unique_file = os.path.join(output_folder, f'grouped_unique_trips_with_times_{key}.csv')

        # Run the analyze_trips function on the cleaned data
        order_result, unique_result, grouped_order_result, grouped_unique_result = analyze_trips(
            csv_output, output_order_file, output_unique_file, output_grouped_order_file, output_grouped_unique_file
        )

        # Optionally, print out the file paths to confirm the output
        print(f"Processed {key}:")
        print(f"Order file saved to: {output_order_file}")
        print(f"Unique file saved to: {output_unique_file}")
        print(f"Grouped order file saved to: {output_grouped_order_file}")
        print(f"Grouped unique file saved to: {output_grouped_unique_file}")

    # Outside the loop: Call map_locations_to_groups after processing all files
    grouped_log_data_file = os.path.join(output_folder, "grouped_Log_data.csv")
    map_locations_to_groups(output_file_path, grouped_log_data_file)

    # Clean and process the grouped data outside the loop
    grouped_log_data_cleaned_file = os.path.join(output_folder, "grouped_Log_dataCleaned.csv")
    remove_invalid_locations_and_fuse(grouped_log_data_file, grouped_log_data_cleaned_file)

    formatted_log_data_cleaned_file = os.path.join(output_folder, "formatted_log_dataCleaned.csv")
    remove_invalid_locations_and_fuse(output_file_path, formatted_log_data_cleaned_file)

    # Analyze full trips data
    output_order_file_full = os.path.join(output_folder, 'individual_order_trips_with_timesFullTrips.csv')
    output_unique_file_full = os.path.join(output_folder, 'individual_unique_trips_with_timeFullTrips.csv')
    output_grouped_order_file_full = os.path.join(output_folder, 'grouped_order_trips_with_timesFullTrips.csv')
    output_grouped_unique_file_full = os.path.join(output_folder, 'grouped_unique_trips_with_timesFullTrips.csv')
    
    order_result_full, unique_result_full, grouped_order_result_full, grouped_unique_result_full = analyze_trips(
        formatted_log_data_cleaned_file, output_order_file_full, output_unique_file_full,
        output_grouped_order_file_full, output_grouped_unique_file_full
    )

    # Analyze grouped trips
    output_order_file_grouped = os.path.join(output_folder, 'individual_order_trips_with_timesGroupedTrips.csv')
    output_unique_file_grouped = os.path.join(output_folder, 'individual_unique_trips_with_timeGroupedTrips.csv')
    output_grouped_order_file_grouped = os.path.join(output_folder, 'grouped_order_trips_with_timesGroupedTrips.csv')
    output_grouped_unique_file_grouped = os.path.join(output_folder, 'grouped_unique_trips_with_timesGroupedTrips.csv')
    
    order_result_grouped, unique_result_grouped, grouped_order_result_grouped, grouped_unique_result_grouped = analyze_trips(
        grouped_log_data_cleaned_file, output_order_file_grouped, output_unique_file_grouped,
        output_grouped_order_file_grouped, output_grouped_unique_file_grouped
    )





