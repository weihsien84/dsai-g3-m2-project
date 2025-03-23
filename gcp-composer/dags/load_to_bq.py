from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta
from typing import List, Dict

# DAG Arguments
default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 3, 22),
}

# Initialize the DAG
with DAG(
    'gcs_to_bigquery',
    default_args=default_args,
    description='load data to BigQuery',
    schedule=None,  # No schedule as it is triggered by upstream DAG
    catchup=False,
    dagrun_timeout=timedelta(minutes=30), # Set longer timeout to allow time for loading
    render_template_as_native_obj=True,
) as dag:
    
    @task
    def get_config(dag_run=None) -> Dict:
        conf=dag_run.conf or {}
        bucket=conf.get('bucket')
        folder=conf.get('gcs_folder')
        if not bucket or not folder:
            raise ValueError('Missing bucket name and GCS folder name in conf')
        return {'bucket': bucket, 'folder': folder}

    @task
    def list_csv_files(config: dict) -> List[str]:
        gcs_hook = GCSHook()
        bucket = config['bucket']
        prefix = config['folder']
        files = gcs_hook.list(bucket_name=bucket, prefix=prefix)
        return files

    @task
    def map_files_to_tables(files: List[str], config: Dict) -> List[Dict]:
        folder = config['folder']
        mappings = []
        for f in files:
            if f.endswith('.csv') and f.startswith(folder):
                filename = f.split('/')[-1]
                table = filename.replace('.csv','')
                mappings.append({
                    'source_objects': [f],
                    'bucket': config['bucket'],
                    'destination_project_dataset_table': f"dsai-g3-m2-project.gcs_ingestion.{table}"
                })
        return mappings
    
    # Task 1: Retrieve parameters from upstream DAG
    config = get_config()

    # Task 2: Get the file list in the GCS bucket
    file_list = list_csv_files(config)

    # Task 3: Map the file name to table name in BigQuery
    mappings = map_files_to_tables(file_list, config)

    # Task 4: Load each csv file as a table to BigQuery
    GCSToBigQueryOperator.partial(
        retries=3,
        retry_delay=timedelta(minutes=2),
        task_id=f"load_to_bq",
        source_format="CSV",
        skip_leading_rows=1,
        max_bad_records=3000,
        field_delimiter=",",
        quote_character='"',
        allow_quoted_newlines=True,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
    ).expand_kwargs(mappings)
