import pandas as pd

def remove_invalid_locations_and_fuse(csv_input, csv_output):
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

# Example usage
csv_input = r"C:\Users\danie\Documents\GlacierNationalPark\tables\grouped_Log_data.csv"
csv_output = r"C:\Users\danie\Documents\GlacierNationalPark\tables\grouped_Log_data3.csv"
remove_invalid_locations_and_fuse(csv_input, csv_output)
