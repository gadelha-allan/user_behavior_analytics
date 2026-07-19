from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import duckdb
import pandas as pd


def get_s3_folder(s3_bucket, s3_folder):
    print(f"Simulando download do bucket {s3_bucket}, pasta {s3_folder} para o ambiente local...")


def create_user_behaviour_metric():

    q = """
    with up as (
      select 
        * 
      from 
        '/opt/airflow/dags/user_purchase.csv'
    ), 
    mr as (
      select 
        * 
      from 
        '/opt/airflow/dags/clean_movie_review/*.parquet'
    ) 
    select 
      up.customer_id, 
      sum(up.quantity * up.unit_price) as amount_spent, 
      sum(
        case when mr.positive_review then 1 else 0 end
      ) as num_positive_reviews, 
      count(mr.cid) as num_reviews 
    from 
      up 
      join mr on up.customer_id = mr.cid 
    group by 
      up.customer_id
    """

    duckdb.sql(q).write_csv("/opt/airflow/dags/behaviour_metrics.csv")
    print("Métricas geradas com sucesso usando DuckDB!")



def check_data_quality():
    
    df = pd.read_csv('/opt/airflow/dags/behaviour_metrics.csv')
    
    if df.empty:
        raise ValueError("Data Quality Falhou: O arquivo de métricas está vazio!")
        

    if df['customer_id'].isnull().any():
        raise ValueError("Data Quality Falhou: Existem IDs de clientes nulos!")
        
    print("Data Quality Aprovado: Os dados estão consistentes e prontos para o Dashboard.")


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
    max_active_runs=1,
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
    
    movie_classifier = BashOperator(
        task_id="movie_classifier",
        bash_command="python /opt/airflow/dags/random_text_classification.py",
    )

    get_movie_review_to_warehouse = PythonOperator(
        task_id="get_movie_review_to_warehouse",
        python_callable=get_s3_folder,
        op_kwargs={"s3_bucket": user_analytics_bucket, "s3_folder": "clean/movie_review"},
    )

    get_user_purchase_to_warehouse = PythonOperator(
        task_id="get_user_purchase_to_warehouse",
        python_callable=get_s3_folder,
        op_kwargs={"s3_bucket": user_analytics_bucket, "s3_folder": "raw/user_purchase"},
    )

    get_user_behaviour_metric = PythonOperator(
        task_id="get_user_behaviour_metric",
        python_callable=create_user_behaviour_metric,
    )
        
    check_quality = PythonOperator(
        task_id="check_data_quality",
        python_callable=check_data_quality,
    )
    
    generate_dashboard = BashOperator(
        task_id="generate_dashboard",
        bash_command="cd /opt/airflow/dags && quarto render dashboard.qmd",
    )


    create_s3_bucket >> [movie_review_to_s3, user_purchase_to_s3]
    
    movie_review_to_s3 >> movie_classifier
    
    movie_classifier >> get_movie_review_to_warehouse
    
    user_purchase_to_s3 >> get_user_purchase_to_warehouse
    
    [get_movie_review_to_warehouse, get_user_purchase_to_warehouse] >> get_user_behaviour_metric >> check_quality >> generate_dashboard


