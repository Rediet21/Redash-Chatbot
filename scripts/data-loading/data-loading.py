import os
import glob
from database import DatabaseConnector
import pandas as pd

def process_and_persist_data(main_directory, database_uri):
    # Create a dictionary to store dataframes for each group
    dataframes = {}

    # Define groups and corresponding directories
    groups = {
        "Demographics": ["Viewer-Data"],
        "Content_Details": ["Content type", "Subtitles and CC"],
        "Viewer_Information": ["New and returning viewers", "Viewership by Date"],
        "Geographic_Information": ["Cities", "Geography"],
        "Device_and_OS": ["Device type", "Operating syzstem"],
        "Subscription_Details": ["Subscription source", "Subscription status"],
        "Sharing_and_Traffic": ["Sharing service", "Traffic source"],
    }

    # Iterate over groups and directories
    for group, directories in groups.items():
        group_data = pd.DataFrame()

        for directory in directories:
            directory_path = os.path.join(main_directory, directory)
            if os.path.exists(directory_path):
                # Use glob to find all CSV files in the directory and subdirectories
                csv_files = glob.glob(os.path.join(directory_path, '**/*.csv'), recursive=True)
                for file_path in csv_files:
                    df = pd.read_csv(file_path)

                    # Categorize DataFrames based on file names within each group
                    if 'chart' in file_path.lower() or 'view' in file_path.lower():
                        group_data = pd.concat([group_data, df], ignore_index=True)

        # Store the categorized DataFrames in the dataframes dictionary
        dataframes[f"{group}"] = group_data

    # Separate the 'Viewer-Data' dataframe into two based on the 'Viewer age' column
    if 'Demographics' in dataframes:
        demographics_df = dataframes['Demographics']

        # Check if the specified columns exist in the DataFrame
        viewer_age_columns = ['Viewer age', 'Views (%)', 'Average view duration', 'Average percentage viewed (%)', 'Watch time (hours) (%)', 'Viewer gender']
        if all(col in demographics_df.columns for col in viewer_age_columns):
            viewer_age_df = demographics_df[viewer_age_columns]
            demographics_df.drop(['Viewer age', 'Viewer gender', 'Views (%)', 'Average view duration', 'Average percentage viewed (%)', 'Watch time (hours) (%)'], axis=1, inplace=True)
        else:
            print("Warning: Viewer age columns not found in Demographics DataFrame. Skipping viewer_age_df creation.")

    # Set up the database connection
    db_connector = DatabaseConnector(database_uri)

    # Persist dataframes into corresponding tables
    if 'Demographics' in dataframes and 'viewer_age_df' in locals():
        db_connector.persist_dataframe(viewer_age_df, 'viewer_age_table')

    db_connector.persist_dataframe(pd.DataFrame(dataframes['Content_Details']), 'content_detail')
    db_connector.persist_dataframe(pd.DataFrame(dataframes['Viewer_Information']).drop(['Watch time (hours)', 'Average view duration'], axis=1), 'demographics')
    db_connector.persist_dataframe(pd.DataFrame(dataframes['Geographic_Information']), 'geography')
    db_connector.persist_dataframe(pd.DataFrame(dataframes['Device_and_OS']), 'device')
    db_connector.persist_dataframe(pd.DataFrame(dataframes['Subscription_Details']), 'subscribe')
    db_connector.persist_dataframe(pd.DataFrame(dataframes['Sharing_and_Traffic']), 'sharing')

if __name__ == "__main__":
    main_directory = "../Data/youtube-data"  
    database_uri = "postgresql://postgres@localhost:15432/postgres"  # Update with your PostgreSQL connection details

    process_and_persist_data(main_directory, database_uri)
