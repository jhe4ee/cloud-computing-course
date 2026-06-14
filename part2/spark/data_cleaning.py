from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, mean, stddev, min, max, when, isnan

spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

# Load dataset (supports both local path and s3a:// OBS path)
df = spark.read.csv("s3a://cloud-course-data-b62c/douban_movies.csv", header=True, inferSchema=True, encoding="UTF-8")

print("=== Schema ===")
df.printSchema()

print("\n=== First 5 rows ===")
df.show(5, truncate=False)

total_before = df.count()
print(f"\nTotal rows before cleaning: {total_before}")

# Missing value analysis
print("\n=== Missing value ratio per column ===")
for c in df.columns:
    missing = df.filter(col(c).isNull() | (col(c) == "") | isnan(col(c))).count()
    print(f"{c}: {missing}/{total_before} ({missing/total_before*100:.1f}%)")

# Strategy 1: dropna - drop rows where original_title or directors is null
df1 = df.dropna(subset=["original_title", "directors"])
after_dropna = df1.count()
print(f"\nAfter dropna (original_title, directors): {after_dropna} rows (dropped {total_before - after_dropna})")

# Strategy 2: fillna - fill missing genres with "未知" and missing countries with "未知"
df_clean = df1.fillna({"genres": "未知", "countries": "未知", "summary": ""})
after_fill = df_clean.count()
print(f"After fillna (genres, countries): {after_fill} rows")

# Final statistics
print("\n=== Clean data basic statistics ===")
df_clean.select("rating_score", "rating_count", "year", "collect_count").describe().show()

print(f"\n=== Row count: before={total_before}, after={after_fill} ===")

spark.stop()
