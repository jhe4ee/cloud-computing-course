from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, round as spark_round, desc, row_number,
    split, explode, year as year_col
)
from pyspark.sql.window import Window
import time

spark = SparkSession.builder.appName("SparkSQLAnalysis").getOrCreate()

# Load and preprocess
df = spark.read.csv("douban_movies.csv", header=True, inferSchema=True, encoding="UTF-8")
df = df.dropna(subset=["original_title", "directors"])
df = df.fillna({"genres": "未知", "countries": "未知"})
df = df.filter(col("year").isNotNull())

# ============================================================
# Query 1: GROUP BY aggregation - average rating by genre
# Split multi-genre (e.g. "犯罪/剧情") into individual rows
# ============================================================
print("=" * 60)
print("Query 1: Average rating score by genre (TOP 15)")
print("=" * 60)
t1 = time.time()
df_genre = df.withColumn("genre", explode(split(col("genres"), "/")))
result1 = df_genre.groupBy("genre") \
    .agg(count("*").alias("movie_count"),
         spark_round(avg("rating_score"), 2).alias("avg_rating")) \
    .filter(col("movie_count") >= 10) \
    .orderBy(desc("avg_rating")) \
    .limit(15)
result1.show(15, truncate=False)
print(f"Query 1 time: {time.time() - t1:.2f}s")

# ============================================================
# Query 2: ORDER BY Top-N - top 20 movies by rating_count
# ============================================================
print("\n" + "=" * 60)
print("Query 2: Top 20 most rated movies")
print("=" * 60)
t2 = time.time()
result2 = df.select("title", "year", "rating_score", "rating_count") \
    .orderBy(desc("rating_count")) \
    .limit(20)
result2.show(20, truncate=False)
print(f"Query 2 time: {time.time() - t2:.2f}s")

# ============================================================
# Query 3: Time dimension trend - avg rating and movie count by year
# ============================================================
print("\n" + "=" * 60)
print("Query 3: Average rating and movie count by year (recent 30 years)")
print("=" * 60)
t3 = time.time()
result3 = df.filter((col("year") >= 1990) & (col("year") <= 2025)) \
    .groupBy("year") \
    .agg(count("*").alias("movie_count"),
         spark_round(avg("rating_score"), 2).alias("avg_rating")) \
    .orderBy("year")
result3.show(50, truncate=False)
print(f"Query 3 time: {time.time() - t3:.2f}s")

# ============================================================
# Query 4: Window function - rank movies by rating within each year
# Top 3 rated movies per recent year
# ============================================================
print("\n" + "=" * 60)
print("Query 4: Top 3 rated movies per year (window function)")
print("=" * 60)
t4 = time.time()
window_spec = Window.partitionBy("year").orderBy(desc("rating_score"))
result4 = df.filter((col("year") >= 2015) & (col("year") <= 2020)) \
    .filter(col("rating_count") >= 10000) \
    .select("year", "title", "rating_score", "rating_count",
            row_number().over(window_spec).alias("rank")) \
    .filter(col("rank") <= 3) \
    .orderBy("year", "rank")
result4.show(30, truncate=False)
print(f"Query 4 time: {time.time() - t4:.2f}s")

print("\nAll queries completed.")
spark.stop()
