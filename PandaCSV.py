from ftplib import FTP, all_errors
import sqlite3
import logging
import pandas as pd
import schedule, time
import os
from io import StringIO
import csv


import os

# Global variable to store the list of downloaded files
downloaded_files = set()

def download_csv_recursive(ftp, remote_dir, local_dir):
    try:
        ftp.cwd(remote_dir)

        files_and_dirs = ftp.nlst()

        print(f"Processing directory: {remote_dir}")

        for item in files_and_dirs:
            item_path = os.path.join(remote_dir, item)

            if ftp.nlst(item) != files_and_dirs:
                print(f"Descending into subdirectory: {item_path}")
                download_csv_recursive(ftp, item_path, local_dir)
            else:
                if item.endswith('.csv') and item not in downloaded_files:
                    local_file_path = os.path.join(local_dir, item)
                    with open(local_file_path, 'wb') as local_file:
                        ftp.retrbinary('RETR ' + item, local_file.write)
                        print(f'Downloaded: {item}')
                        # Add the file to the set of downloaded files
                        downloaded_files.add(item)

        # Check for new files in the local directory that are not in the set of downloaded files
        new_files = set(files_and_dirs) - downloaded_files
        for new_file in new_files:
            if new_file.endswith('.csv'):
                new_file_path = os.path.join(remote_dir, new_file)
                local_file_path = os.path.join(local_dir, new_file)
                with open(local_file_path, 'wb') as local_file:
                    ftp.retrbinary('RETR ' + new_file_path, local_file.write)
                    print(f'Downloaded new file: {new_file}')
                    # Add the new file to the set of downloaded files
                    downloaded_files.add(new_file)

    except Exception as e:
        print(f"Error: {e}")

def download_csv_files_from_ftp(ftp_host, ftp_user, ftp_password, remote_base_directory, local_directory):
    global downloaded_files  # Use the global variable

    # Connect to the FTP server
    with FTP() as ftp:
        ftp.connect(ftp_host)
        ftp.login(user=ftp_user, passwd=ftp_password)

        # Start the recursive download
        download_csv_recursive(ftp, remote_base_directory, local_directory)

        print('Download complete.')

# Function to delete files in a directory
def delete_files(directory):
    for file in os.listdir(directory):
        file_path = os.path.join(directory, file)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"Deleted: {file}")
        except Exception as e:
            print(f"Error deleting file {file}: {e}")


def merge_csv_files(input_dir, output_file):
    all_data = pd.DataFrame()

    for file in os.listdir(input_dir):
        if file.endswith('.csv'):
            file_path = os.path.join(input_dir, file)
            data = pd.read_csv(file_path)
            all_data = pd.concat([all_data, data], ignore_index=True)

    all_data.to_csv(output_file, index=False)
    print(f'Merged CSV file saved at: {output_file}')

    return all_data  # Add this line to return the merged data



def create_players_table(cursor,conn):
    try:
        conn = sqlite3.connect('Data.db')
        cursor = conn.cursor()

        # Create the Players table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Players (
                PitchNo INTEGER,
                Date TEXT,
                Time TEXT,
                PAofInning INTEGER,
                PitchofPA INTEGER,
                Pitcher TEXT,
                PitcherId TEXT,
                PitcherThrows TEXT,
                PitcherTeam TEXT,
                Batter TEXT,
                BatterId TEXT,
                BatterSide TEXT,
                BatterTeam TEXT,
                PitcherSet TEXT,
                Inning INTEGER,
                TopBottom TEXT,
                Outs INTEGER,
                Balls INTEGER,
                Strikes INTEGER,
                TaggedPitchType TEXT,
                AutoPitchType TEXT,
                PitchCall TEXT,
                KorBB TEXT,
                TaggedHitType TEXT,
                PlayResult TEXT,
                OutsOnPlay INTEGER,
                RunsScored INTEGER,
                Notes TEXT,
                RelSpeed REAL,
                VertRelAngle REAL,
                HorzRelAngle REAL,
                SpinRate REAL,
                SpinAxis REAL,
                Tilt REAL,
                RelHeight REAL,
                RelSide REAL,
                Extension REAL,
                VertBreak REAL,
                InducedVertBreak REAL,
                HorzBreak REAL,
                PlateLocHeight REAL,
                PlateLocSide REAL,
                ZoneSpeed REAL,
                VertApprAngle REAL,
                HorzApprAngle REAL,
                ZoneTime REAL,
                ExitSpeed REAL,
                Angle REAL,
                Direction REAL,
                HitSpinRate REAL,
                PositionAt110X REAL,
                PositionAt110Y REAL,
                PositionAt110Z REAL,
                Distance REAL,
                LastTrackedDistance REAL,
                Bearing REAL,
                HangTime REAL,
                pfxx REAL,
                pfxz REAL,
                x0 REAL,
                y0 REAL,
                z0 REAL,
                vx0 REAL,
                vy0 REAL,
                vz0 REAL,
                ax0 REAL,
                ay0 REAL,
                az0 REAL,
                HomeTeam TEXT,
                AwayTeam TEXT,
                Stadium TEXT,
                Level TEXT,
                League TEXT,
                GameID TEXT,
                PitchUID TEXT,
                EffectiveVelo REAL,
                MaxHeight REAL,
                MeasuredDuration REAL,
                SpeedDrop REAL,
                PitchLastMeasuredX REAL,
                PitchLastMeasuredY REAL,
                PitchLastMeasuredZ REAL,
                ContactPositionX REAL,
                ContactPositionY REAL,
                ContactPositionZ REAL,
                GameUID TEXT,
                UTCDate TEXT,
                UTCTime TEXT,
                LocalDateTime TEXT,
                UTCDateTime TEXT,
                AutoHitType TEXT,
                System TEXT,
                HomeTeamForeignID TEXT,
                AwayTeamForeignID TEXT,
                GameForeignID TEXT,
                Catcher TEXT,
                CatcherId TEXT,
                CatcherThrows TEXT,
                CatcherTeam TEXT,
                PlayID TEXT,
                PitchTrajectoryXc0 REAL,
                PitchTrajectoryXc1 REAL,
                PitchTrajectoryXc2 REAL,
                PitchTrajectoryYc0 REAL,
                PitchTrajectoryYc1 REAL,
                PitchTrajectoryYc2 REAL,
                PitchTrajectoryZc0 REAL,
                PitchTrajectoryZc1 REAL,
                PitchTrajectoryZc2 REAL,
                HitSpinAxis REAL,
                HitTrajectoryXc0 REAL,
                HitTrajectoryXc1 REAL,
                HitTrajectoryXc2 REAL,
                HitTrajectoryXc3 REAL,
                HitTrajectoryXc4 REAL,
                HitTrajectoryXc5 REAL,
                HitTrajectoryXc6 REAL,
                HitTrajectoryXc7 REAL,
                HitTrajectoryXc8 REAL,
                HitTrajectoryYc0 REAL,
                HitTrajectoryYc1 REAL,
                HitTrajectoryYc2 REAL,
                HitTrajectoryYc3 REAL,
                HitTrajectoryYc4 REAL,
                HitTrajectoryYc5 REAL,
                HitTrajectoryYc6 REAL,
                HitTrajectoryYc7 REAL,
                HitTrajectoryYc8 REAL,
                HitTrajectoryZc0 REAL,
                HitTrajectoryZc1 REAL,
                HitTrajectoryZc2 REAL,
                HitTrajectoryZc3 REAL,
                HitTrajectoryZc4 REAL,
                HitTrajectoryZc5 REAL,
                HitTrajectoryZc6 REAL,
                HitTrajectoryZc7 REAL,
                HitTrajectoryZc8 REAL,
                ThrowSpeed REAL,
                PopTime REAL,
                ExchangeTime REAL,
                TimeToBase REAL,
                CatchPositionX REAL,
                CatchPositionY REAL,
                CatchPositionZ REAL,
                ThrowPositionX REAL,
                ThrowPositionY REAL,
                ThrowPositionZ REAL,
                BasePositionX REAL,
                BasePositionY REAL,
                BasePositionZ REAL,
                ThrowTrajectoryXc0 REAL,
                ThrowTrajectoryXc1 REAL,
                ThrowTrajectoryXc2 REAL,
                ThrowTrajectoryYc0 REAL,
                ThrowTrajectoryYc1 REAL,
                ThrowTrajectoryYc2 REAL,
                ThrowTrajectoryZc0 REAL,
                ThrowTrajectoryZc1 REAL,
                ThrowTrajectoryZc2 REAL,
                PitchReleaseConfidence REAL,
                PitchLocationConfidence REAL,
                PitchMovementConfidence REAL,
                HitLaunchConfidence REAL,
                HitLandingConfidence REAL,
                CatcherThrowCatchConfidence REAL,
                CatcherThrowReleaseConfidence REAL,
                CatcherThrowLocationConfidence REAL
            )
        ''')

        conn.commit()
        conn.close()
        print("Table 'Players' created successfully.")

    except Exception as e:
        print(f"An error occurred: {str(e)}")

# Call the function to create the table



def insert_csv_data_into_database(database_name, table_name, csv_columns, merged_data):
    try:
        # Connect to SQLite database
        conn = sqlite3.connect(database_name)
        cursor = conn.cursor()

        # Call the function to create the table if it doesn't exist
        create_players_table(cursor, conn)

        # Define column data types
        column_data_types = {
            col: 'TEXT'  # Change 'TEXT' to the appropriate data type for each column
            for col in csv_columns
        }

        # Insert data into the database
        merged_data.to_sql(table_name, conn, if_exists='replace', index=False, dtype=column_data_types)

        # Commit the changes and close the connection
        conn.commit()
        conn.close()

        logging.info(f"Inserted CSV data into {table_name} in {database_name}")

    except Exception as e:
        logging.error(f"Error inserting data into database: {str(e)}")






def main():
    logging.basicConfig(level=logging.INFO)

    ftp_host = 'ftp.trackmanbaseball.com'
    ftp_user = 'Louisiana Monroe'
    ftp_password = 'URjIMKVuqr'
    remote_base_directory = '/v3/2024'
    local_directory = 'D:/Baseball/all'
    database_name = 'PlayersCSV.db'
    table_name = 'Players'
    csv_columns = ['PitchNo','Date', 'Time', 'PAofInning','PitchofPA','Pitcher','PitcherId',
                'PitcherThrows','PitcherTeam','Batter',
                'BatterId','BatterSide','BatterTeam',
                'PitcherSet','Inning','TopBottom',
                'Outs','Balls','Strikes',
                'TaggedPitchType','AutoPitchType','PitchCall',
                'KorBB','TaggedHitType','PlayResult',
                'OutsOnPlay','RunsScored','Notes',
                'RelSpeed','VertRelAngle','HorzRelAngle',
                'SpinRate','SpinAxis','Tilt',
                'RelHeight','RelSide','Extension',
                'VertBreak','InducedVertBreak','HorzBreak',
                'PlateLocHeight','PlateLocSide','ZoneSpeed','VertApprAngle','HorzApprAngle','ZoneTime','ExitSpeed','Angle','Direction','HitSpinRate','PositionAt110X','PositionAt110Y','PositionAt110Z',
                'Distance','LastTrackedDistance','Bearing','HangTime','pfxx','pfxz','x0','y0','z0','vx0','vy0','vz0','ax0',
                'ay0','az0','HomeTeam','AwayTeam','Stadium','Level','League','GameID','PitchUID','EffectiveVelo',
                'MaxHeight','MeasuredDuration','SpeedDrop','PitchLastMeasuredX','PitchLastMeasuredY','PitchLastMeasuredZ','ContactPositionX','ContactPositionY','ContactPositionZ','GameUID',
                'UTCDate','UTCTime','LocalDateTime','UTCDateTime','AutoHitType','System','HomeTeamForeignID','AwayTeamForeignID','GameForeignID','Catcher','CatcherId','CatcherThrows','CatcherTeam',
                'PlayID','PitchTrajectoryXc0','PitchTrajectoryXc1','PitchTrajectoryXc2','PitchTrajectoryYc0','PitchTrajectoryYc1','PitchTrajectoryYc2','PitchTrajectoryZc0','PitchTrajectoryZc1',
                'PitchTrajectoryZc2','HitSpinAxis','HitTrajectoryXc0','HitTrajectoryXc1','HitTrajectoryXc2','HitTrajectoryXc3','HitTrajectoryXc4','HitTrajectoryXc5',
                'HitTrajectoryXc6','HitTrajectoryXc7','HitTrajectoryXc8','HitTrajectoryYc0','HitTrajectoryYc1','HitTrajectoryYc2','HitTrajectoryYc3','HitTrajectoryYc4','HitTrajectoryYc5',
                'HitTrajectoryYc6','HitTrajectoryYc7','HitTrajectoryYc8','HitTrajectoryZc0','HitTrajectoryZc1','HitTrajectoryZc2','HitTrajectoryZc3','HitTrajectoryZc4',
                'HitTrajectoryZc5','HitTrajectoryZc6','HitTrajectoryZc7','HitTrajectoryZc8','ThrowSpeed','PopTime','ExchangeTime','TimeToBase','CatchPositionX',
                'CatchPositionY','CatchPositionZ','ThrowPositionX','ThrowPositionY','ThrowPositionZ','BasePositionX','BasePositionY','BasePositionZ','ThrowTrajectoryXc0',
                'ThrowTrajectoryXc1','ThrowTrajectoryXc2','ThrowTrajectoryYc0','ThrowTrajectoryYc1','ThrowTrajectoryYc2','ThrowTrajectoryZc0',
                'ThrowTrajectoryZc1','ThrowTrajectoryZc2','PitchReleaseConfidence','PitchLocationConfidence','PitchMovementConfidence','HitLaunchConfidence',
                'HitLandingConfidence','CatcherThrowCatchConfidence','CatcherThrowReleaseConfidence','CatcherThrowLocationConfidence']
    
    local_directory = 'D:/Baseball/all'  # Update with your actual directory
    output_file = 'D:/Baseball/merged_output.csv'  # Update with your desired output file path

    def job():
        try:
            # Download CSV files from FTP and merge them
            download_csv_files_from_ftp(ftp_host, ftp_user, ftp_password, remote_base_directory, local_directory)
            merged_data = merge_csv_files(local_directory, output_file)

            if merged_data is not None and not merged_data.empty:  # Check if merged_data is not empty
                # Connect to SQLite database
                conn = sqlite3.connect(database_name)
                cursor = conn.cursor()

                # Call the function to create the table if it doesn't exist
                create_players_table(cursor, conn)
                print(f"Number of CSV columns: {len(csv_columns)}")
                print(f"Number of data columns: {len(merged_data.columns)}")

                # Insert data into the database
                insert_csv_data_into_database(database_name, table_name, csv_columns, merged_data)
                # Commit the changes and close the connection
                conn.commit()
                conn.close()

                logging.info("CSV data transfer and database insertion successful.")

                # Delete files after successful insertion
                delete_files(local_directory)
            else:
                logging.info("No new data to import.")
        except Exception as e:
            logging.error(f"An error occurred: {str(e)}")


    # Schedule the job to run every 24 hours
    schedule.every(24).hours.do(job)

    # Run the job initially
    job()

    # Keep the script running indefinitely to execute scheduled jobs
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
