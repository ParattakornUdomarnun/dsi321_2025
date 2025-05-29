import os
import time
import datetime
import requests
import pandas as pd
import pyarrow
from prefect import flow, task # Prefect flow and task decorators
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo


@task
def fetch_firms() :
    MAP_KEY = '5e8bad8d50fa1ca84ea72175e2bace34' #key from guy.80647@gmail.com
    thai_url = 'https://firms.modaps.eosdis.nasa.gov/api/country/csv/' + MAP_KEY + '/MODIS_NRT/THA/2'

    try :
        df_thai = pd.read_csv(thai_url)
        # Ensure "acq_time" is a string, then pad with zeros
        df_thai["acq_time"] = df_thai["acq_time"].astype(str).str.zfill(4)
        # Convert to HH:MM format
        df_thai["acq_time"] = pd.to_datetime(df_thai["acq_time"], format="%H%M").dt.time
        df_thai['acq_datetime'] = pd.to_datetime(df_thai['acq_date'].astype(str) + ' ' + df_thai['acq_time'].astype(str), format='%Y-%m-%d %H:%M:%S')
        df_thai['acq_datetime_th'] = df_thai['acq_datetime'].dt.tz_localize('GMT').dt.tz_convert('Asia/Bangkok')
        df_thai["acq_year"] = pd.to_datetime(df_thai["acq_datetime_th"]).dt.year
        df_thai["acq_month"] = pd.to_datetime(df_thai["acq_datetime_th"]).dt.month
        df_thai["acq_day"] = pd.to_datetime(df_thai["acq_datetime_th"]).dt.day
        df_thai["acq_hour"] = pd.to_datetime(df_thai["acq_datetime_th"]).dt.hour
        df_thai["acq_minute"] = pd.to_datetime(df_thai["acq_datetime_th"]).dt.minute
        df_thai["timestamp"] = df_thai["acq_datetime_th"].dt.strftime("%Y-%m-%d %H:%M:%S")

        for col in df_thai.columns:
            if df_thai[col].dtype == 'object':
                df_thai[col] = df_thai[col].astype('string')
        return df_thai


    except KeyError as e:
        print(f"Error processing data: Missing key {e}")
        return None
    

@flow(name="firmsapi-flow", log_prints=True)
def firmsapi_flow() :
    # lakeFS credentials from your docker-compose.yml
    ACCESS_KEY = "access_key"
    SECRET_KEY = "secret_key"

    # lakeFS endpoint (running locally)
    lakefs_endpoint = "http://lakefs-dev:8000/"

    # lakeFS repository, branch, and file path
    repo = "weather"
    branch = "main"
    path = "firms.parquet"

    # Construct the full lakeFS S3-compatible path
    lakefs_s3_path = f"s3a://{repo}/{branch}/{path}"

    # Configure storage_options for lakeFS (S3-compatible)
    storage_options = {
        "key": ACCESS_KEY,
        "secret": SECRET_KEY,
        "client_kwargs": {
            "endpoint_url": lakefs_endpoint
        }
    }
    df_firms = fetch_firms() 
    # Write DataFrame to a directory "output_parquet" partitioned by retrieval_time
    df_firms.to_parquet(
        lakefs_s3_path,
         storage_options=storage_options,
         partition_cols=["acq_year","acq_month","acq_day","acq_hour","acq_minute"],   # <-- crucial for partitioning by retrieval_time
         engine="pyarrow",
         index=False,
    )


if __name__ == "__main__":
    firmsapi_flow()