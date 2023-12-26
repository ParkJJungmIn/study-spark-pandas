
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import TimestampType
from dotenv import load_dotenv
import os


load_dotenv()
# AWS 자격 증명 설정
access_key = os.getenv("access_key",'test')
secret_key = os.getenv("secret_key",'test')
endpoint_url = "https://kr.object.ncloudstorage.com"

# Spark 세션 초기화 및 S3 설정 추가
# spark = SparkSession.builder.appName("csvToParquet") \
#     .config("spark.hadoop.fs.s3a.access.key", access_key) \
#     .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
#     .config("spark.hadoop.fs.s3a.endpoint", endpoint_url) \
#     .getOrCreate()


spark = SparkSession.builder.appName("csvToParquet") \
    .config("spark.hadoop.fs.s3a.access.key", access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
    .config("spark.hadoop.fs.s3a.endpoint", endpoint_url) \
    .config("spark.driver.extraClassPath", "/Users/parkjeongmin/workspace/event-project/aws-java-sdk-bundle-1.12.481.jar") \
    .config("spark.executor.extraClassPath", "/Users/parkjeongmin/workspace/event-project/aws-java-sdk-bundle-1.12.481.jar") \
    .getOrCreate()


# spark = SparkSession.builder.appName("csvToParquet") \
#     .config("spark.hadoop.fs.s3a.access.key", access_key) \
#     .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
#     .config("spark.hadoop.fs.s3a.endpoint", endpoint_url) \
#     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.353") \
#     .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
#     .getOrCreate()

    # .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \

# S3에서 CSV 파일 읽기
csv_file_path = "s3a://data-lake/bronze/eventsim.csv"
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# 'ts' 필드를 날짜 형식으로 변환
df = df.withColumn("date", to_date((col("ts") / 1000).cast(TimestampType())))

# 날짜별로 데이터를 분할하고 Parquet으로 저장
output_base_path = "s3a://data-lake/silver/eventsim/"
df.write.partitionBy("date").parquet(output_base_path)

# Spark 세션 종료
spark.stop()



# import boto3
# import pandas as pd
# from io import StringIO
# from pyspark.sql import SparkSession

# # Boto3 클라이언트 초기화
# s3_client = boto3.client('s3', endpoint_url='https://kr.object.ncloudstorage.com', 
#                          aws_access_key_id='', 
#                          aws_secret_access_key='')

# # S3에서 데이터 읽기
# bucket_name = 'data-lake'
# csv_file_key = 'bronze/eventsim_big.csv'
# response = s3_client.get_object(Bucket=bucket_name, Key=csv_file_key)
# csv_content = response['Body'].read().decode('utf-8')

# # Pandas DataFrame으로 변환
# df_pd = pd.read_csv(StringIO(csv_content))

# # Spark 세션 초기화
# spark = SparkSession.builder.appName("boto3ToSpark").getOrCreate()

# # Pandas DataFrame을 Spark DataFrame으로 변환
# df_spark = spark.createDataFrame(df_pd)

# # Spark로 데이터 처리
# # 예: 날짜별로 데이터를 분할하고 Parquet으로 저장
# df_spark.write.partitionBy("date").parquet("path_to_save_parquet_files")

# # Spark 세션 종료
# spark.stop()