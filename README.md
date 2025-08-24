How to Build/Run the Code

- Prerequisites: You need to have Python 3 installed on your system.

- Dependencies: The script requires the requests and pandas libraries. You can install them using pip:
- 
pip install requests pandas


- Run the Script: The script takes a single positional argument: the S3 URL of the dataset. Run it from your terminal like this:

python data_pipeline.py https://s3-us-west-2.amazonaws.com/com.guild.us-west-2.public-data/project-data/the-movies-dataset.zip

Output and Deliverables

1. Upon successful execution, the script will create a new directory named processed_data in the same location. This directory will contain:

    data/: A subdirectory containing the processed CSV files for each table in your data model (movies.csv, production_companies.csv, genres.csv, etc.).

    error_log.txt: A log file that captures all print statements and any errors encountered during processing. This will be empty on a successful run but will contain error details if something goes wrong.

    revenue_by_genre_by_year.sql: A text file containing the SQL query to gather movie genre details, as required by the prompt.
