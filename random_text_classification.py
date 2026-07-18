import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType


spark = SparkSession.builder \
    .appName("MovieReviewClassifier") \
    .master("local[*]") \
    .getOrCreate()


def classify_text_mock(text):
    return random.choice([True, False])

classify_udf = udf(classify_text_mock, BooleanType())

input_path = "/opt/airflow/dags/movie_review.csv"
output_path = "/opt/airflow/dags/clean_movie_review"

try:

    df = spark.read.csv(input_path, header=True, inferSchema=True)
 
    classified_df = df.withColumn("positive_review", classify_udf(df["review_str"]))
    
    final_df = classified_df.select("cid", "positive_review")

    final_df.write.mode("overwrite").parquet(output_path)
    print("Processamento concluído com sucesso! Meus dados limpos foram gerados.")

except Exception as e:
    print(f"Ops, ocorreu um erro no meu processamento Spark: {e}")

finally:

    spark.stop()
