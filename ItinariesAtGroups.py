import pandas as pd
import os

# Location filters (your dictionary)
location_filters = {
    "Apgar": ["Apgar_village", "Apgar_campground", "Rocky_point_trail", "Apgar_VC", "Apgar_VC_parking"],
    "Avalanche": ["Avalanche_trail", "Trail_of_Cedars", "Avalanche_parking", "Shuttle_Avalanche"],
    "Logan": ["Logan_pass_VC", "Oberlin_bend", "Hidden_lake_trail", "Loop_highline_grinnell_glac", "Oberlin_climbing",
              "Logan_pass_parking", "Siyeh_bend_parking", "Hidden_lake_overlook", "Shuttle_Logan_pass"],
    "LakeMcDonald": ["Shuttle_Lake_macdonald", "Lake_macdonald_lodge"],
    "Loop": ["Loop_parking", "Shuttle_Loop"],
    "Waterfalls": ["Gunsight_trailhead_stMary_falls", "Baring_falls_parking", "St_mary_falls_parking", "StMary_falls_trail",
                   "Baring_falls_trail", "Baring_falls_to_st_mary_falls", "Baring_falls_to_sun_point",
                   "Viriginia_falls_trail", "Shuttle_St_mary_falls", "Jackson_glac_overlook", "Shuttle_Jackson_glac_overlook",
                   "Shuttle_Sunrift_gorge"],
    "SunPoint": ["Sun_point_nature_trail", "Sun_point_parking", "Shuttle_Sun_point"],
    "RisingSun": ["Rising_sun_boat_dock", "Rising_sun_campground", "Rising_sun_picnic_area", "Rising_sun",
                  "Shuttle_Rising_Sun", "Otokomi_lake_trail"],
    "StMary": ["St_mary_campground", "St_mary_VC", "St_mary_VC_parking"],
}

# Function to map location to group
def map_location_to_group(location, location_filters):
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
                group_match = map_location_to_group(location, location_filters)

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

# Input and output file paths
input_file = r"C:\Users\danie\Downloads\formatted_log_data_5min clip.csv"
output_folder = 'C:/Users/danie/Documents/GlacierNationalPark/tables/grouped_locations_by_group2'

# Process the CSV and save separate files by group
process_grouped_locations_to_separate_files(input_file, output_folder, location_filters)
