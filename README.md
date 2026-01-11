# Stack Overflow Developer Survey ETL Pipeline

This project implements an ETL (Extract, Transform, Load) pipeline in Python to process and analyze data from the Stack Overflow Developer Survey to obtain metrics about the most used technologies and the most used platforms.

## Description

The `main.py` script automates the data workflow:

1.  **Extract**:
    *   Automatically downloads the survey dataset from a cloud URL.
    *   Saves the raw data in the `data/` folder.

2.  **Transform**:
    *   Filters only columns of interest (Country, Age, Languages, Databases, Platforms).
    *   Standardizes country names.
    *   Converts age ranges to average numerical values.
    *   Drops rows with null values in key columns.
    *   Removes duplicate records.

3.  **Load**:
    *   Saves the cleaned dataset in CSV (`survey_clean.csv`) and Parquet (`survey_clean.parquet`) formats in the `output/` folder.
    *   Generates and saves specific metrics in CSV files, such as popular technologies and demographic statistics.

## Requirements

*   Python 3.x
*   pandas
*   pyarrow

## Installation

1.  Clone the repository or download the files.
2.  Install the necessary dependencies by running:

```bash
pip install -r requirements.txt
```

## Usage

Run the main script from the terminal:

```bash
python main.py
```

The process will download the data, process it, and print information about progress and the size reduction achieved with the Parquet format.

## Docker
```
docker build -t trends-techs .
docker run trends-techs
```
## Output

The resulting files will be saved in the `output/` folder:

*   **Processed Data**:
    *   `survey_clean.csv`: Cleaned data in CSV format.
    *   `survey_clean.parquet`: Cleaned data in Parquet format (more efficient).
*   **Metrics and Reports**:
    *   `top10_db.csv`: Top 10 desired databases.
    *   `top10_db_future.csv`: Top 10 databases currently used.
    *   `top10_lang.csv`: Top 10 desired languages.
    *   `top10_lang_future.csv`: Top 10 languages currently used.
    *   `top10_platform.csv`: Top 10 desired platforms.
    *   `top10_platform_future.csv`: Top 10 platforms currently used.
    *   `countries_age.csv`: Developer count and average age by country.
    *   `dev_age.csv`: Developer age distribution.

## Author
[Josue Armenta] [2026-01-08]
