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

    #Standardize Data
    df['Country'] = df['Country'].replace({"Congo, Republic of the...": "Congo",
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

    df['Age'] = df['Age'].map({
        'Under 18 years old': 17,
        '18-24 years old': 21,
        '25-34 years old': 29.5,
        '35-44 years old': 39.5,
        '45-54 years old': 49.5,
        '55-64 years old': 59.5,
        '65 years or older': 65,
        'Prefer not to say': None
    })

    #Handle Nulls Values 
    before = len(df)
    df = df.dropna(subset=['Country', 'Age', 'LanguageHaveWorkedWith', 'LanguageWantToWorkWith', 'DatabaseHaveWorkedWith', 'DatabaseWantToWorkWith','PlatformHaveWorkedWith', 'PlatformWantToWorkWith'])
    after = len(df)
    print("Rows removed:", before - after)

    #Handle Duplicates
    duplicate_rows = df[df.duplicated(keep=False)]
    if len(duplicate_rows) > 0:
        df_cleaned = df.drop_duplicates(keep=False)
        print("Dataset size:", len(df))
        print("Duplicate rows removed:", len(duplicate_rows))
        print("Cleaned dataset size:", len(df_cleaned))
    else:
        print("No duplicate rows found in the dataset.")

    #Copy Only Columns of Interest from Cleaned Data
    df_cleaned = df.copy()
    df_cleaned = df_cleaned[['Country', 'Age', 'LanguageHaveWorkedWith', 'LanguageWantToWorkWith', 'DatabaseHaveWorkedWith', 'DatabaseWantToWorkWith','PlatformHaveWorkedWith', 'PlatformWantToWorkWith']]
    return df_cleaned

def load(df_cleaned : pd.DataFrame, output_dir : str = 'output'):
    """Load data to a CSV file."""
    os.makedirs(output_dir, exist_ok=True)
    print("Load: Saving Data...")
    df_cleaned.to_csv(f'{output_dir}/survey_clean.csv', index=False)
    df_cleaned.to_parquet(f'{output_dir}/survey_clean.parquet', index=False)
    
    csv_size = os.path.getsize('output/survey_clean.csv') / 1024
    parquet_size = os.path.getsize('output/survey_clean.parquet') / 1024

    print(f"Tamaño CSV: {csv_size:.1f} KB")
    print(f"Tamaño Parquet: {parquet_size:.1f} KB")
    print(f"Parquet is {csv_size/parquet_size:.1f}x smaller than CSV")

    #Metrics
    #Top 10 Database Want To Work With
    db_want = df_cleaned['DatabaseWantToWorkWith']
    db_want = db_want.str.split(';')
    db_want = db_want.explode()
    top10_db = db_want.value_counts().head(10).reset_index(name='Count')
    top10_db.columns = ['DatabaseWantToWorkWith', 'Count']

    #Top 10 Database Have Worked With
    db_have = df_cleaned['DatabaseHaveWorkedWith']
    db_have = db_have.str.split(';')
    db_have = db_have.explode()
    top10_db_have = db_have.value_counts().head(10).reset_index(name='Count')
    top10_db_have.columns = ['DatabaseHaveWorkedWith', 'Count']

    #Top 10 Language Want To Work With
    lang_want = df_cleaned['LanguageWantToWorkWith']
    lang_want = lang_want.str.split(';')
    lang_want = lang_want.explode()
    top10_lang = lang_want.value_counts().head(10).reset_index(name='Count')
    top10_lang.columns = ['LanguageWantToWorkWith', 'Count']

    #Top 10 Language Have Worked With
    lang_have = df_cleaned['LanguageHaveWorkedWith']
    lang_have = lang_have.str.split(';')
    lang_have = lang_have.explode()
    top10_lang_have = lang_have.value_counts().head(10).reset_index(name='Count')
    top10_lang_have.columns = ['LanguageHaveWorkedWith', 'Count']

    #Top 10 Platform Want To Work With
    platform_want = df_cleaned['PlatformWantToWorkWith']
    platform_want = platform_want.str.split(';')
    platform_want = platform_want.explode()
    top10_platform = platform_want.value_counts().head(10).reset_index(name='Count')
    top10_platform.columns = ['PlatformWantToWorkWith', 'Count']

    #Top 10 Platform Have Worked With
    platform_have = df_cleaned['PlatformHaveWorkedWith']
    platform_have = platform_have.str.split(';')
    platform_have = platform_have.explode()
    top10_platform_have = platform_have.value_counts().head(10).reset_index(name='Count')
    top10_platform_have.columns = ['PlatformHaveWorkedWith', 'Count']

    #Countries by developers and average age
    countries = df_cleaned['Country'].value_counts().reset_index(name='Count')
    countries.columns = ['Country', 'Count']
    countries_age = countries.merge(df_cleaned.groupby('Country')['Age'].mean().reset_index(), on='Country')
    countries_age.columns = ['Country', 'Count', 'Average Age']
    countries_age = countries_age.sort_values(by='Count', ascending=False)

    #Number of developers by age
    dev_age = df_cleaned['Age'].value_counts().reset_index(name='Count').sort_values(by='Count', ascending=False)
    dev_age.columns = ['Age', 'Count']

    #Save Metrics
    top10_db.to_csv(f'{output_dir}/top10_db.csv', index=False)
    top10_db_have.to_csv(f'{output_dir}/top10_db_future.csv', index=False)
    top10_lang.to_csv(f'{output_dir}/top10_lang.csv', index=False)
    top10_lang_have.to_csv(f'{output_dir}/top10_lang_future.csv', index=False)
    top10_platform.to_csv(f'{output_dir}/top10_platform.csv', index=False)
    top10_platform_have.to_csv(f'{output_dir}/top10_platform_future.csv', index=False)
    countries_age.to_csv(f'{output_dir}/countries_age.csv', index=False)
    dev_age.to_csv(f'{output_dir}/dev_age.csv', index=False)

    print("Data saved successfully.")

def main():
    print("ETL Process Started")
    table = extract('data')
    df_cleaned = transform(table)
    load(df_cleaned)
    print("ETL Process Completed")

if __name__ == '__main__':
    main()

