import pandas as pd

def analyze_trips(file_path, output_order_file, output_unique_file, output_grouped_order_file, output_grouped_unique_file):
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

# File paths
file_path = r"C:\Users\danie\Documents\GlacierNationalPark\tables\LogData.csv"
output_order_file = 'individual_order_trips_with_times5.csv'
output_unique_file = 'individual_unique_trips_with_time5.csv'
output_grouped_order_file = 'grouped_order_trips_with_times5.csv'
output_grouped_unique_file = 'grouped_unique_trips_with_times5.csv'

# Call the function
order_result, unique_result, grouped_order_result, grouped_unique_result = analyze_trips(file_path, output_order_file, output_unique_file, output_grouped_order_file, output_grouped_unique_file)
