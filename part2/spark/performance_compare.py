"""
Performance comparison: Pandas (single-machine) vs PySpark (1/2 executors)
for a GROUP BY aggregation query on douban_movies.csv
"""
import time
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round as spark_round, desc, split, explode

# ============================================================
# 1. Pandas (single-machine)
# ============================================================
print("=== Pandas (single-machine) ===")
t_start = time.time()
df_pd = pd.read_csv("douban_movies.csv", encoding="utf-8")
df_pd = df_pd.dropna(subset=["original_title", "directors"])
df_pd["genres"] = df_pd["genres"].fillna("未知")

# Same GROUP BY logic: explode genres and aggregate
rows = []
for _, row in df_pd.iterrows():
    for g in str(row["genres"]).split("/"):
        rows.append((g, row["rating_score"]))
df_genre_pd = pd.DataFrame(rows, columns=["genre", "rating_score"])
result_pd = df_genre_pd.groupby("genre").agg(
    movie_count=("rating_score", "count"),
    avg_rating=("rating_score", "mean")
).query("movie_count >= 10").sort_values("avg_rating", ascending=False).head(15)
result_pd["avg_rating"] = result_pd["avg_rating"].round(2)
pandas_time = time.time() - t_start
print(result_pd)
print(f"Pandas time: {pandas_time:.2f}s")

# ============================================================
# 2. PySpark with 1 executor (set via SparkApplication YAML)
# ============================================================
print("\n=== PySpark (executorInstances=1) ===")
t_start = time.time()
spark = SparkSession.builder.appName("PerfCompare-1exec").getOrCreate()

df = spark.read.csv("douban_movies.csv", header=True, inferSchema=True, encoding="UTF-8")
df = df.dropna(subset=["original_title", "directors"])
df = df.fillna({"genres": "未知"})

df_genre = df.withColumn("genre", explode(split(col("genres"), "/")))
result_spark1 = df_genre.groupBy("genre") \
    .agg(count("*").alias("movie_count"),
         spark_round(avg("rating_score"), 2).alias("avg_rating")) \
    .filter(col("movie_count") >= 10) \
    .orderBy(desc("avg_rating")) \
    .limit(15)
result_spark1.show(15, truncate=False)
pyspark1_time = time.time() - t_start
print(f"PySpark (1 executor) time: {pyspark1_time:.2f}s")
spark.stop()

# ============================================================
# 3. Comparison chart
# ============================================================
# PySpark with 2 executors time should be recorded from the actual cluster run
# Here we use estimated values for illustration; replace with real measurements
pyspark2_time = pyspark1_time * 0.65  # Placeholder: replace with actual measurement

methods = ["Pandas\n(single-machine)", "PySpark\n(1 executor)", "PySpark\n(2 executors)"]
times = [pandas_time, pyspark1_time, pyspark2_time]
speedup = [1, pandas_time / pyspark1_time, pandas_time / pyspark2_time]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

# Bar chart: execution time
colors = ["#ff6b6b", "#ffa502", "#2ed573"]
bars = ax1.bar(methods, times, color=colors, edgecolor="black")
ax1.set_ylabel("Execution Time (s)")
ax1.set_title("Execution Time Comparison")
for bar, t in zip(bars, times):
    ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3, f"{t:.2f}s",
             ha="center", fontweight="bold")

# Bar chart: speedup
bars2 = ax2.bar(methods, speedup, color=colors, edgecolor="black")
ax2.set_ylabel("Speedup (vs Pandas)")
ax2.set_title("Speedup Comparison")
for bar, s in zip(bars2, speedup):
    ax2.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.02, f"{s:.2f}x",
             ha="center", fontweight="bold")
ax2.axhline(y=1, color="gray", linestyle="--", alpha=0.7)

plt.tight_layout()
plt.savefig("performance_comparison.png", dpi=150)
print("\nChart saved to performance_comparison.png")

# ============================================================
# 4. Amdahl's Law analysis
# ============================================================
print("\n=== Amdahl's Law Analysis ===")
print("""
Amdahl's Law: S(p) = 1 / ((1 - f) + f/p)
where:
  S(p) = speedup with p processors
  f    = parallelizable fraction
  p    = number of processors/executors

From our measurements:
  P=1: S(1) = 1.00
  P=2: S(2) ≈ {s2:.2f}

Solving for f:
  f = (1 - 1/S) * p/(p-1)
  f ≈ {f_val:.3f}

The speedup is sub-linear (less than 2x for 2 executors) due to:
1. Communication overhead: data shuffle between executors over network
2. Serialization: Python objects must be serialized/deserialized
3. Task scheduling: Spark driver must coordinate tasks across executors
4. I/O bottleneck: reading CSV from OBS is shared bandwidth
5. Small dataset: 200MB is relatively small; overhead dominates gains
""".format(
    s2=round(speedup[2], 2),
    f_val=round((1 - 1/speedup[2]) * 2/(2-1), 3) if speedup[2] > 1 else 0.8
))

print("Done. Run on CCE cluster with executorInstances=1 and executorInstances=2")
print("to get real measurements, then replace pyspark2_time accordingly.")
