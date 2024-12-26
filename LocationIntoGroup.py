import pandas as pd

# Editable dictionary mapping locations to groups
location_to_group = {
    "Apgar": ["Apgar_village", "Apgar_campground", "Rocky_point_trail", "Apgar_VC", "Apgar_VC_parking"],
    "Avalanche": ["Avalanche_trail", "Trail_of_Cedars", "Avalanche_parking"],
    "Logan": ["Logan_pass_VC", "Hidden_lake_trail", "Loop_highline_grinnell_glac", "Oberlin_climbing", "Logan_pass_parking", "Siyeh_bend_parking", "Hidden_lake_overlook"],
    "SunPoint": ["Gunsight_trailhead_stMary_falls", "Baring_falls_parking", "St_mary_falls_parking", "StMary_falls_trail", "Baring_falls_trail", "Baring_falls_to_st_mary_falls", "Baring_falls_to_sun_point", "Viriginia_falls_trail", "Sun_point_parking", "Sun_point_nature_trail"],
    "Loop": ["Loop_parking", "Loop_highline_grinnell_glac"],
    "LakeMcDonald": ["Lake_macdonald_lodge"],
    "RisingSun": ["Rising_sun_boat_dock", "Rising_sun_campground", "Rising_sun_picnic_area", "Rising_sun"],
    "St Mary": ["St_mary_campground", "St_mary_VC", "St_mary_VC_parking"],
    "Not Sorted": ["Fish_creek", "Jackson_glac_overlook", "Moose_country", "Wild_goose_island", "Siyeh_bend_trail", "Big_bend", "Snyder_ridge_trail", "Lunch_creek", "Otokomi_lake_trail", "Siyeh_pass_trail", "Red_eagle_trail", "Red_rock", "Lunch_creek_parking", "Oberlin_bend"]
}

def map_locations_to_groups(input_file_path, output_file_path):
    # Read the input CSV file
    df = pd.read_csv(input_file_path)

    # Function to map location to group, keeping blanks as blanks
    def map_location_to_group(location):
        if pd.isna(location) or location == "":  # Check for blanks or NaN
            return ""
        for group, locations in location_to_group.items():
            if location in locations:
                return group
        return "Unknown"  # Return "Unknown" if location doesn't match any group

    # Loop through the columns and apply the mapping only to the location columns
    location_columns = [col for col in df.columns if col.startswith('location_')]

    for loc_col in location_columns:
        # Apply the mapping function to the location columns
        df[loc_col] = df[loc_col].apply(map_location_to_group)

    # Save the modified DataFrame to the output CSV file
    df.to_csv(output_file_path, index=False)

    print(f"New group-based CSV file saved to: {output_file_path}")

map_locations_to_groups('C:\\Users\\danie\\Downloads\\formatted_log_data_5min clip.csv',
'C:\\Users\\danie\\Documents\\GlacierNationalPark\\tables\\grouped_Log_data.csv')
