from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
import pandas as pd
from pathlib import Path
from prefect_gcp.cloud_storage import GcsBucket


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(url: str) -> pd.DataFrame:
    df = pd.read_csv(url)
    # print(f"columns:\n{df.dtypes}")
    # print(f"rows: {len(df)}")
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    # print(f"columns:\n{df.dtypes}")
    # print(f"rows: {len(df)}")
    return df

@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression='gzip')
    return path

@task(log_prints=True)
def write_to_gcs(path: Path) -> None:
    gcp_bucket_block = GcsBucket.load("ss-gcs")
    gcp_bucket_block.upload_from_path(from_path=path, to_path=f"{path}")

@flow(log_prints=True)
def web_to_gcs(color: str, year: int, month: int):
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_to_gcs(path)

if __name__ == "__main__":
    color = 'green'
    year = 2020
    month = 11
    web_to_gcs(color, year, month)
