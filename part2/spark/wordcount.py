from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WordCount").getOrCreate()

sample_text = [
    "Spark is a fast and general engine for large scale data processing",
    "Apache Spark provides high level APIs in Java Scala Python and R",
    "Spark runs on Hadoop Mesos Kubernetes standalone or in the cloud",
    "It can access diverse data sources such as HDFS S3 and HBase",
    "Machine learning is supported through MLlib on Spark",
    "Spark SQL enables querying structured data using SQL",
    "Streaming data can be processed with Spark Streaming",
    "Graph processing is available through GraphX on Spark",
    "Cloud computing enables on demand resource provisioning",
    "Kubernetes is a portable extensible open source platform",
    "Spark on Kubernetes is the future of big data processing",
    "Data engineers use Spark for ETL and data pipelines",
    "Python is the most popular language for data science",
    "Spark supports batch processing and real time streaming",
]
lines = spark.sparkContext.parallelize(sample_text)

word_counts = (
    lines.flatMap(lambda line: line.split())
         .map(lambda word: (word, 1))
         .reduceByKey(lambda a, b: a + b)
         .sortBy(lambda x: x[1], ascending=False)
)

print("Top 10 words:", word_counts.take(10))
spark.stop()
