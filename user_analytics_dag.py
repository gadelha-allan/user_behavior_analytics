from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

user_analytics_bucket = "user-analytics"

with DAG(
    'user_analytics_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='DAG to Pull user data and movie review data to analyze their behaviour'
) as dag:
    
    create_s3_bucket = S3CreateBucketOperator(
        task_id="create_s3_bucket",
        bucket_name=user_analytics_bucket,
        aws_conn_id="aws_default"
    )

    movie_review_to_s3 = LocalFilesystemToS3Operator(
        task_id="movie_review_to_s3",
        filename="/opt/airflow/dags/movie_review.csv",
        dest_key="raw/movie_review.csv",
        dest_bucket=user_analytics_bucket,
        aws_conn_id="aws_default",
        replace=True,
    )


    user_purchase_to_s3 = SqlToS3Operator(
        task_id="user_purchase_to_s3",
        sql_conn_id="postgres_default",
        aws_conn_id="aws_default",
        query="select * from retail.user_purchase",
        s3_bucket=user_analytics_bucket,
        s3_key="raw/user_purchase/user_purchase.csv",
        replace=True,
    )
    create_s3_bucket >> [movie_review_to_s3, user_purchase_to_s3]
