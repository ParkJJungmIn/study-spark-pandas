from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType
import pyspark.sql.functions as F

# Spark 세션 초기화
spark = SparkSession.builder \
    .appName("JsonToMySQL") \
    .config("spark.jars", "/Users/parkjeongmin/workspace/event-project/mysql-connector-java-8.0.28.jar") \
    .config("spark.driver.extraClassPath", "/Users/parkjeongmin/workspace/event-project/mysql-connector-java-8.0.28.jar") \
    .getOrCreate()

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

df = spark.read.json("event.data.json", schema=schema)

df.write \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost/mydatabase") \
    .option("dbtable", "user_song_count") \
    .option("user", "root") \
    .option("password", "my-secret-pw") \
    .mode("append") \
    .save()

spark.stop()
