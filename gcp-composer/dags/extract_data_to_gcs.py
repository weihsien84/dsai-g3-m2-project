from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import zipfile
import os
from datetime import datetime, timedelta

GCS_BUCKET_NAME = 'dsai-g3-m2-landing-bucket' # GCS bucket name
dataset = 'olistbr/brazilian-ecommerce' # Kaggle dataset
data_folder = dataset.split("/")[1] # Dataset name
dataset_path = f'/tmp/data/{data_folder}.zip'  # Adjust with the actual zip name
extract_path = f'/tmp/data/{data_folder}/' # Path to extract the files

# DAG Arguments
default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 3, 22),
}

# Initialize the DAG
with DAG(
    'kaggle_to_gcs',
    default_args=default_args,
    description='extract data and load to GCS',
    schedule="0 0 */2 * *",  # Schedule run every 2 days
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
) as dag:
    
    # Task 1: Use BashOperator to run Kaggle command and download the dataset
    download_data_from_kaggle = BashOperator(
        task_id='download_data_from_kaggle',
        bash_command=f'mkdir -p /tmp/data && kaggle datasets download -d {dataset} -p /tmp/data',
    )

    # Task 2: Unzip the downloaded file using PythonOperator
    def unzip_data():

        if not os.path.exists(dataset_path):
            raise FileNotFoundError(f"File {dataset_path} not found!")
        os.makedirs(extract_path, exist_ok=True)

        with zipfile.ZipFile(dataset_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)

    unzip_data_task = PythonOperator(
        task_id='unzip_data',
        python_callable=unzip_data,
    )

    # Task 3: Upload extracted files to GCS
    def upload_to_gcs(extract_path, data_folder,**kwargs):

        gcs_hook = GCSHook()
        csv_files = [file for file in os.listdir(extract_path) if file.endswith('.csv')]

        # Upload each CSV file to GCS
        for csv_file in csv_files:
            local_file_path = os.path.join(extract_path, csv_file)
            gcs_file_path = f"{data_folder}/{csv_file}"
            
            # Delete old file if exists
            if gcs_hook.exists(bucket_name=GCS_BUCKET_NAME, object_name=gcs_file_path):
                gcs_hook.delete(bucket_name=GCS_BUCKET_NAME, object_name=gcs_file_path)

            gcs_hook.upload(
                bucket_name=GCS_BUCKET_NAME,
                object_name=gcs_file_path,
                filename=local_file_path
            )
    
    upload_to_gcs_task= PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
        op_args=[extract_path, data_folder],
    )

    # Task 4: Trigger next DAG
    trigger_next = TriggerDagRunOperator(
        task_id="trigger_downstream_dag",
        trigger_dag_id="gcs_to_bigquery",
        conf={
            "gcs_folder": f"{data_folder}",
            "bucket": GCS_BUCKET_NAME
        },
    )

    # Task Dependencies
    download_data_from_kaggle >> unzip_data_task >> upload_to_gcs_task >> trigger_next
