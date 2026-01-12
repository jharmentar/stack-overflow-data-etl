# main.py Stack Overflow Developer Survey Pipeline

import pandas as pd
import os


def extract(data_dir : str = 'data') -> dict:
    """Extract data from a CSV file."""
    os.makedirs(data_dir, exist_ok=True)
    print("Extract: Downloading Data...")
    table = {}
    url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/VYPrOu0Vs3I0hKLLjiPGrA/survey-data-with-duplicate.csv'
    table['Survey'] = pd.read_csv(url)
    print(f"   Survey: {len(table['Survey'])} rows")
    table['Survey'].to_csv(f'{data_dir}/survey.csv', index=False)
    return table

def transform(table : dict) -> pd.DataFrame:
    """Transform data."""
    df = table['Survey'].copy()
    
    #Copy only columns of interest
    df_use = df[['Country', 
    'LanguageHaveWorkedWith', 
    'LanguageWantToWorkWith', 
    'DatabaseHaveWorkedWith', 
    'DatabaseWantToWorkWith',
    'PlatformHaveWorkedWith', 
    'PlatformWantToWorkWith', 
    'WebframeHaveWorkedWith', 
    'WebframeWantToWorkWith']].copy()
    
    #Standardize data
    df_use['Country'] = df_use['Country'].replace({"Congo, Republic of the...": "Congo",
    "Democratics People's Republic of Korea": "North Korea",
    "Hong Kong (S.A.R.)": "Hong Kong",
    "Iran, Islamic Republic of...": "Iran",
    "Micronesia, Federated States of...": "Micronesia",
    "Republic of Korea": "South Korea",
    "Republic of Moldova": "Moldova",
    "Republic of North Macedonia": "North Macedonia",
    "Russian Federation": "Russia",
    "Syrian Arab Republic": "Syria",
    "United Kingdom of Great Britain and Northern Ireland": "United Kingdom",
    "United Republic of Tanzania": "Tanzania",
    "United States of America": "United States",
    "Venezuela, Bolivarian Republic of...": "Venezuela",
    "Viet Nam": "Vietnam"})

    #Handle Nulls Values 
    before = len(df_use)
    df_use = df_use.dropna()
    after = len(df_use)
    print("Rows removed:", before - after)

    #Handle Duplicates
    duplicate_rows = df_use[df_use.duplicated(keep=False)]
    if len(duplicate_rows) > 0:
        df_cleaned = df_use.drop_duplicates(keep=False)
        print("Dataset size:", len(df_use))
        print("Duplicate rows removed:", len(duplicate_rows))
        print("Cleaned dataset size:", len(df_cleaned))
    else:
        print("No duplicate rows found in the dataset.")

    return df_cleaned

def load(df_cleaned : pd.DataFrame, output_dir : str = 'output'):
    """Load data to a CSV file."""
    os.makedirs(output_dir, exist_ok=True)

    print("Load: Saving Data...")
    df_cleaned.to_csv(f'{output_dir}/survey_clean.csv', index=False)
    df_cleaned.to_parquet(f'{output_dir}/survey_clean.parquet', index=False)
    
    csv_size = os.path.getsize('output/survey_clean.csv') / 1024
    parquet_size = os.path.getsize('output/survey_clean.parquet') / 1024

    print(f"CSV Size: {csv_size:.1f} KB")
    print(f"Parquet Size: {parquet_size:.1f} KB")
    print(f"Parquet is {csv_size/parquet_size:.1f}x smaller than CSV")

    #Metrics
    #Top 10 Database Want To Work With
    db_want = df_cleaned['DatabaseWantToWorkWith'].str.split(';').explode()
    top10_db_want = db_want.value_counts().head(10).reset_index(name='Count')
    top10_db_want.columns = ['DatabaseWantToWorkWith', 'Count']

    #Top 10 Database Have Worked With
    db_have = df_cleaned['DatabaseHaveWorkedWith'].str.split(';').explode()
    top10_db_have = db_have.value_counts().head(10).reset_index(name='Count')
    top10_db_have.columns = ['DatabaseHaveWorkedWith', 'Count']

    #Top 10 Language Want To Work With
    lang_want = df_cleaned['LanguageWantToWorkWith'].str.split(';').explode()
    top10_lang_want = lang_want.value_counts().head(10).reset_index(name='Count')
    top10_lang_want.columns = ['LanguageWantToWorkWith', 'Count']

    #Top 10 Language Have Worked With
    lang_have = df_cleaned['LanguageHaveWorkedWith'].str.split(';').explode()
    top10_lang_have = lang_have.value_counts().head(10).reset_index(name='Count')
    top10_lang_have.columns = ['LanguageHaveWorkedWith', 'Count']

    #Top 10 Platform Want To Work With
    platform_want = df_cleaned['PlatformWantToWorkWith'].str.split(';').explode()
    top10_platform_want = platform_want.value_counts().head(10).reset_index(name='Count')
    top10_platform_want.columns = ['PlatformWantToWorkWith', 'Count']

    #Top 10 Platform Have Worked With
    platform_have = df_cleaned['PlatformHaveWorkedWith'].str.split(';').explode()
    top10_platform_have = platform_have.value_counts().head(10).reset_index(name='Count')
    top10_platform_have.columns = ['PlatformHaveWorkedWith', 'Count']   

    #Top 10 WebFrame Have Worked With
    webframe_have = df_cleaned['WebframeHaveWorkedWith'].str.split(';').explode()
    top10_webframe_have = webframe_have.value_counts().head(10).reset_index(name='Count')
    top10_webframe_have.columns = ['WebframeHaveWorkedWith', 'Count']

    #Top 10 WebFrame Want To Work With
    webframe_want = df_cleaned['WebframeWantToWorkWith'].str.split(';').explode()
    top10_webframe_want = webframe_want.value_counts().head(10).reset_index(name='Count')
    top10_webframe_want.columns = ['WebframeWantToWorkWith', 'Count']

    #Countries by developers
    countries = df_cleaned['Country'].value_counts().reset_index(name='Count')
    countries.columns = ['Country', 'Count']

    #Save Metrics
    top10_db_want.to_csv(f'{output_dir}/top10_db_future.csv', index=False)
    top10_db_have.to_csv(f'{output_dir}/top10_db.csv', index=False)
    top10_lang_want.to_csv(f'{output_dir}/top10_lang_future.csv', index=False)
    top10_lang_have.to_csv(f'{output_dir}/top10_lang.csv', index=False)
    top10_platform_want.to_csv(f'{output_dir}/top10_platform_future.csv', index=False)
    top10_platform_have.to_csv(f'{output_dir}/top10_platform.csv', index=False)
    top10_webframe_want.to_csv(f'{output_dir}/top10_webframe_future.csv', index=False)
    top10_webframe_have.to_csv(f'{output_dir}/top10_webframe.csv', index=False)
    countries.to_csv(f'{output_dir}/countries.csv', index=False)

    print("Data saved successfully.")

def main():
    print("ETL Process Started")
    table = extract('data')
    df_cleaned = transform(table)
    load(df_cleaned)
    print("ETL Process Completed")

if __name__ == '__main__':
    main()

