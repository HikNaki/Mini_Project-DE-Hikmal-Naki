import os
import asyncio
import nbformat
import requests
import pandas as pd
import firebase_admin
from datetime import datetime
from dotenv import load_dotenv
from prefect import task, flow
from firebase_admin import storage
from prefect.logging import get_logger
from firebase_admin import credentials
from nbconvert.preprocessors import ExecutePreprocessor

logger = get_logger(__name__)
pd.option_context('mode.chained_assignment', None)

# Import Data from API
@task(retries=3, retry_delay_seconds=60, task_run_name="Fetch API")
def fetch_api():
    try:
        api_data = {
        'SP.POP.TOTL': 'data_source/api_population_data.csv',
        'NY.GDP.MKTP.CD': 'data_source/api_gdp_data.csv',
        'SP.RUR.TOTL.ZS': 'data_source/api_rural_population_data.csv',
        'EG.ELC.ACCS.ZS': 'data_source/api_electricity_access_data.csv'
        }
        
        for indicator, file_path in api_data.items():
            url = f'http://api.worldbank.org/v2/countries/all/indicators/{indicator}/?format=json&per_page=1000'
            columns = ['country.value', 'countryiso3code', 'date', 'value']
            all_data = []

            response = requests.get(url)
            total_pages = response.json()[0]['pages']

            for page in range(1, total_pages + 1):
                page_url = f'{url}&page={page}'
                response = requests.get(page_url)
                data = response.json()[1]
                page_data = pd.json_normalize(data)[columns]
                all_data.append(page_data)
            
            if all_data:
                df = pd.concat(all_data, ignore_index=True)
                df.columns = ['Country Name', 'Country Code', 'Year', indicator]
                df = df[(df['Year'] >= '2018') & (df['Year'] <= '2023')]
                df_pivot = df.pivot(index=['Country Name', 'Country Code'], columns='Year', values=indicator).reset_index()
                df_pivot.columns.name = None
                df_pivot.to_csv(file_path, index=False)
                logger.info(f"Data successfully fetched from API for indicator {indicator}")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

# Execute Notebook 
@task(retries=3, retry_delay_seconds=60, task_run_name="Execute Notebook")
def notebook_etl():
    if os.name == 'nt':  # Check for Windows platform
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        notebook_filename = "code/etl_pipelines.ipynb"
        with open(notebook_filename, encoding="utf-8") as f:
            nb = nbformat.read(f, as_version=4)
        ep = ExecutePreprocessor(timeout=600)
        output_filename="code/notebook.ipynb"
        ep.preprocess(nb, {'metadata': {'path': 'code/'}})
        with open(output_filename, 'w', encoding='utf-8') as f:
            nbformat.write(nb, f)
        logger.info(f"Notebook '{notebook_filename}' executed successfully and saved to '{output_filename}'.")

    except Exception as e:
        logger.error(f"Error executing notebook: {e}")
        raise

# Load To Cloud Storage
@task(retries=3, retry_delay_seconds=60, task_run_name="Load to Cloud Storage")
def load_to_storage():
    try:
        load_dotenv()
        key = os.getenv("ACCOUNT_KEY")
        
        storage_bucket = os.getenv("FIREBASE_STORAGE")
        if not firebase_admin._apps:
            cred = credentials.Certificate(key)
            firebase_admin.initialize_app(cred, {"storageBucket": storage_bucket})
        
        bucket = storage.bucket()
        current_date = datetime.now().strftime("%d-%m-%Y")
        file_path = 'data_final'
        file_name = 'world_data.csv'
        blob = bucket.blob(f"{current_date}/{file_name}")
        blob.upload_from_filename(f"{file_path}/{file_name}")
        logger.info(f"Data has been successfully loaded to the storage")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise


@flow(log_prints=True)
def data_pipeline():
    #run task 1
    task_1 = fetch_api.submit()

    #run task 2
    task_2 =notebook_etl.submit(wait_for=[task_1])

    #run task 3
    task_3 = load_to_storage.submit(wait_for=[task_2])

if __name__ == "__main__":
    data_pipeline.serve(name="my-deployment-v3",
                        cron="0 9 * * 1",
                        tags=["data pipeline"],
                        description='Data Pipeline'
)
