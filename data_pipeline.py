import sys
import os
import zipfile
import json
import pandas as pd
import requests
from io import BytesIO

# --- Constants ---
# These constants define the names of the output files and directories.
PROCESSED_DATA_DIR = "processed_data"
LOG_FILE_NAME = "error_log.txt"
SQL_QUERY_FILE_NAME = "revenue_by_genre_by_year.sql"
MOVIES_CSV = "movies_metadata.csv"


def get_data_from_s3(s3_url: str) -> BytesIO:
    """
    Downloads a zip file from a given S3 URL and returns it as a BytesIO object.

    Args:
        s3_url (str): The S3 URL of the zipped dataset.

    Returns:
        BytesIO: A BytesIO object containing the content of the downloaded zip file.

    Raises:
        requests.exceptions.RequestException: If the download fails.
    """
    try:
        print(f"Downloading data from {s3_url}...")
        response = requests.get(s3_url)
        response.raise_for_status()  # This will raise an exception for bad status codes
        print("Download complete.")
        return BytesIO(response.content)
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to download file from S3: {e}")


def parse_json_string(json_str: str) -> list:
    """
    Parses a JSON string representation of a list of dictionaries.

    Args:
        json_str (str): The JSON string to parse.

    Returns:
        list: A list of dictionaries parsed from the string, or an empty list on error.
    """
    try:
        # The JSON strings in the dataset are not standard and require a small fix.
        # They often use single quotes instead of double quotes.
        return json.loads(json_str.replace("'", "\""))
    except json.JSONDecodeError:
        return []


def main():
    """
    Main function to run the data processing pipeline.
    It handles downloading, parsing, and transforming the data.
    """
    # 1. Input: Get S3 URL from command line arguments
    if len(sys.argv) < 2:
        print("Error: S3 endpoint not provided.")
        print("Usage: python script_name.py s3_endpoint_url")
        sys.exit(1)

    s3_endpoint = sys.argv[1]

    # Clean up any previous output directory
    if os.path.exists(PROCESSED_DATA_DIR):
        import shutil
        shutil.rmtree(PROCESSED_DATA_DIR)

    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

    # Redirect stdout and stderr to a log file
    log_file_path = os.path.join(PROCESSED_DATA_DIR, LOG_FILE_NAME)
    with open(log_file_path, "w") as log_file:
        sys.stdout = log_file
        sys.stderr = log_file

        try:
            print("Starting data processing pipeline...")

            # 2. Download and unzip the data
            zip_data = get_data_from_s3(s3_endpoint)
            with zipfile.ZipFile(zip_data, 'r') as zip_ref:
                zip_ref.extract(MOVIES_CSV, ".")

            # 3. Read the main movies CSV file
            movies_df = pd.read_csv(MOVIES_CSV, low_memory=False)

            # 4. Data Transformation and Normalization

            # Initialize empty lists to build normalized dataframes
            production_companies_list = []
            genres_list = []
            movie_genres_list = []
            movie_production_companies_list = []

            # Iterate through each movie record
            for index, row in movies_df.iterrows():
                try:
                    movie_id = row['id']

                    # Process genres
                    genres_data = parse_json_string(row['genres'])
                    for genre in genres_data:
                        genre_id = genre['id']
                        genre_name = genre['name']
                        genres_list.append({'genre_id': genre_id, 'name': genre_name})
                        movie_genres_list.append({'movie_id': movie_id, 'genre_id': genre_id})

                    # Process production companies
                    companies_data = parse_json_string(row['production_companies'])
                    for company in companies_data:
                        company_id = company['id']
                        company_name = company['name']
                        production_companies_list.append({'company_id': company_id, 'name': company_name})
                        movie_production_companies_list.append({'movie_id': movie_id, 'company_id': company_id})
                except Exception as e:
                    print(f"Error processing row with movie ID '{row['id']}': {e}")

            # Create DataFrames from the lists and drop duplicates
            production_companies_df = pd.DataFrame(production_companies_list).drop_duplicates().set_index('company_id')
            genres_df = pd.DataFrame(genres_list).drop_duplicates().set_index('genre_id')
            movie_genres_df = pd.DataFrame(movie_genres_list).drop_duplicates()
            movie_production_companies_df = pd.DataFrame(movie_production_companies_list).drop_duplicates()

            # Clean and prepare the main movies dataframe
            # Drop unnecessary columns to keep the model clean
            movies_df = movies_df[['id', 'title', 'release_date', 'budget', 'revenue', 'popularity']]
            movies_df = movies_df.rename(columns={'id': 'movie_id'})
            movies_df['year'] = pd.to_datetime(movies_df['release_date'], errors='coerce').dt.year
            movies_df = movies_df.drop_duplicates(subset=['movie_id']).set_index('movie_id')

            # 5. Output: Save processed data to CSV files
            output_dir_path = os.path.join(PROCESSED_DATA_DIR, "data")
            os.makedirs(output_dir_path, exist_ok=True)

            movies_df.to_csv(os.path.join(output_dir_path, "movies.csv"))
            production_companies_df.to_csv(os.path.join(output_dir_path, "production_companies.csv"))
            genres_df.to_csv(os.path.join(output_dir_path, "genres.csv"))
            movie_genres_df.to_csv(os.path.join(output_dir_path, "movie_genres.csv"))
            movie_production_companies_df.to_csv(os.path.join(output_dir_path, "movie_production_companies.csv"))

            # 6. Output: Write the required SQL query to a file
            sql_query = """
            SELECT
                EXTRACT(YEAR FROM M.release_date) AS release_year,
                G.name AS genre_name,
                SUM(M.revenue) AS total_revenue
            FROM
                movies M
            JOIN
                movie_genres MG ON M.movie_id = MG.movie_id
            JOIN
                genres G ON MG.genre_id = G.genre_id
            GROUP BY
                release_year, genre_name
            ORDER BY
                release_year, total_revenue DESC;
            """

            with open(os.path.join(PROCESSED_DATA_DIR, SQL_QUERY_FILE_NAME), "w") as sql_file:
                sql_file.write(sql_query)

            print("\nData processing complete!")
            print(f"Processed data saved to: {os.path.join(PROCESSED_DATA_DIR, 'data')}")
            print(f"SQL query saved to: {os.path.join(PROCESSED_DATA_DIR, SQL_QUERY_FILE_NAME)}")
            print(f"Error log saved to: {log_file_path}")

        except Exception as e:
            print(f"\nAn unrecoverable error occurred: {e}")
            sys.exit(1)
        finally:
            # Restore original stdout and stderr
            sys.stdout = sys.__stdout__
            sys.stderr = sys.__stderr__
            # Clean up the downloaded CSV file
            if os.path.exists(MOVIES_CSV):
                os.remove(MOVIES_CSV)


if __name__ == "__main__":
    main()
