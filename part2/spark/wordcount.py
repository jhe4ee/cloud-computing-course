import os
from pyspark.sql import SparkSession

ak = os.environ.get("OBS_AK", "")
sk = os.environ.get("OBS_SK", "")
endpoint = os.environ.get("OBS_ENDPOINT", "obs.cn-north-4.myhuaweicloud.com")

spark = SparkSession.builder \
    .appName("WordCount") \
    .config("spark.hadoop.fs.s3a.access.key", ak) \
    .config("spark.hadoop.fs.s3a.secret.key", sk) \
    .config("spark.hadoop.fs.s3a.endpoint", endpoint) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

lines = spark.sparkContext.textFile("s3a://cloud-course-data-b62c/sample.txt")

word_counts = (
    lines.flatMap(lambda line: line.split())
         .map(lambda word: (word, 1))
         .reduceByKey(lambda a, b: a + b)
         .sortBy(lambda x: x[1], ascending=False)
)

print("Top 10 words:", word_counts.take(10))
spark.stop()
