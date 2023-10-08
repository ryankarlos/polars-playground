import json
import pendulum
import polars as pl
import pyarrow.parquet as pq
import s3fs
from airflow.decorators import dag, task
import logging


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def tutorial_taskflow_api():
    """
    ### TaskFlow API Tutorial Documentation
    """
    @task()
    def load_lazy_data_from_s3():
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline. In this case, getting data is simulated by reading from a
        hardcoded JSON string.
        """
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'


        fs = s3fs.S3FileSystem()
        bucket = "<YOUR_BUCKET>"
        path = "<YOUR_PATH>"

        dataset = pq.ParquetDataset(f"s3://{bucket}/{path}", filesystem=fs)
        df_parquet = pl.scan_pyarrow_dataset(dataset)
        print(df_parquet.schema)
        return df_parquet
    @task(multiple_outputs=True)
    def transform(order_data_dict: dict):
        """
        #### Transform task
        A simple Transform task which takes in the collection of order data and
        computes the total order value.
        """
        total_order_value = 0

        for value in order_data_dict.values():
            total_order_value += value

        return {"total_order_value": total_order_value}
    @task()
    def write_to_bucket(df: float):
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task and
        instead of saving it to end user review, just prints it out.
        """

        # set up
        fs = s3fs.S3FileSystem(profile='s3_full_access')

        # write parquet
        with fs.open(f'bucket/result.parquet', mode='wb') as f:
            df.write_parquet(f)
    load_data = load_lazy_data_from_s3()
    transform_df = transform(load_data)
    write_to_bucket(transform_df)
tutorial_taskflow_api()


