from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType
from pyspark.sql.functions import col, from_unixtime, to_date, count
from pyspark.sql import functions as F
# Spark 세션 초기화
spark = SparkSession.builder \
    .appName("JsonToMySQLDailyAggregate") \
    .config("spark.jars", "/Users/parkjeongmin/workspace/event-project/mysql-connector-java-8.0.28.jar") \
    .config("spark.driver.extraClassPath", "/Users/parkjeongmin/workspace/event-project/mysql-connector-java-8.0.28.jar") \
    .getOrCreate()

# 스키마 정의
schema = StructType([
    StructField("ts", LongType(), True),
    StructField("userId", StringType(), True),
    StructField("sessionId", IntegerType(), True),
    StructField("page", StringType(), True),
    StructField("auth", StringType(), True),
    StructField("method", StringType(), True),
    StructField("status", IntegerType(), True),
    StructField("level", StringType(), True),
    StructField("itemInSession", IntegerType(), True),
    StructField("location", StringType(), True),
    StructField("userAgent", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("firstName", StringType(), True),
    StructField("registration", LongType(), True),
    StructField("gender", StringType(), True),
    StructField("tag", StringType(), True),
    StructField("artist", StringType(), True),
    StructField("song", StringType(), True),
    StructField("length", FloatType(), True)
])

# JSON 파일 읽기
df = spark.read.json("event.data.json", schema=schema)

# 타임스탬프를 날짜로 변환
df = df.withColumn("date", to_date(from_unixtime(col("ts") / 1000)))

# 일 단위로 집계
daily_agg_df = df.groupBy("date", "level", "location", "gender").agg(
    F.countDistinct("userId").alias("unique_user_count"),  # 고유한 사용자 수
    F.countDistinct("sessionId").alias("unique_session_count"),  # 고유한 세션 수
    F.count("song").alias("total_song_plays"),  # 총 노래 재생 횟수
    F.sum("length").alias("total_play_time")  # 총 재생 시간
)

# 데이터 MySQL에 저장
daily_agg_df.write \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost/mydatabase") \
    .option("dbtable", "user_daily_song_count") \
    .option("user", "root") \
    .option("password", "my-secret-pw") \
    .mode("append") \
    .save()

# Spark 세션 종료
spark.stop()
