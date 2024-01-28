# Baseball Data Processing Script

This Python script is designed to download CSV files from an FTP server, merge them, and insert the data into a SQLite database. The script assumes that the CSV files contain baseball-related data.

## Requirements

- Python 3.x
- pandas
- sqlite3
- schedule
- logging

## Usage

1. Clone this repository to your local machine.

    ```bash
    git clone https://github.com/your-username/baseball-data-processing.git
    ```

2. Install the required dependencies.

    ```bash
    pip install pandas sqlite3 schedule
    ```

3. Update the script with your FTP server credentials and file paths.

    - `ftp_host`: The hostname of your FTP server.
    - `ftp_user`: The username for FTP login.
    - `ftp_password`: The password for FTP login.
    - `remote_base_directory`: The remote directory on the FTP server where CSV files are located.
    - `local_directory`: The local directory where CSV files will be downloaded.
    - `database_name`: The name of the SQLite database where data will be inserted.
    - `table_name`: The name of the table in the SQLite database.
    - `csv_columns`: A list of column names expected in the CSV files.
    - `output_file`: The path to the merged output CSV file.

4. Run the script.

    ```bash
    python script_name.py
    ```

5. To Connect SQL Lite Database to PowerBi to get data

	- Visit this Youtube URL: https://youtu.be/6bmTdIeu5rA

## Additional Information

- The script uses the `schedule` library to set up a recurring job for data processing. You can adjust the schedule according to your needs.

- Make sure to create the SQLite database table with the necessary schema. The script includes a function (`create_players_table`) for this purpose.

- The merged CSV file is saved at the specified output path (`output_file`).

- Logging is implemented to capture any errors or information during script execution.

## Notes

- This script assumes a specific structure for the CSV files and database schema. Adjustments may be needed based on your actual data format.

- Ensure that the required Python libraries are installed before running the script.

- The FTP server credentials and other sensitive information should be handled securely.
