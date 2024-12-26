import re
import pandas as pd

def parse_logs(log_file_path):
    log_entries = []
    current_id = None
    
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
            # Extract entry and exit times for both POI and Shuttle Stops
            elif 'entered polygon' in line:
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
                    log_entries.append({
                        'id': current_id,
                        'location': location,
                        'enter_time': enter_time,
                        'exit_time': exit_time,
                        'duration': duration
                    })

    return log_entries

def create_csv(log_entries, output_file_path):
    # Create a DataFrame
    df = pd.DataFrame(log_entries)

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


# File paths
log_file_path = r"C:\Users\danie\Documents\scripts\finalCode\DoubleFilterLogs.txt"  # Update to your log file path
output_file_path = r"C:\Users\danie\Documents\scripts\finalCode\DoubleFilterLogs2.csv"  # Update to your desired output path

# Process logs and create CSV
log_entries = parse_logs(log_file_path)
create_csv(log_entries, output_file_path)
