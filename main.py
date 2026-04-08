from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, expr

spark = SparkSession.builder.master("local[*]").appName("spark-dev-env").getOrCreate()


df = spark.createDataFrame(
    [
        Row(name="san", salary=1500),
        Row(name="ana", salary=2000),
        Row(name="shu", salary=1000),
    ]
)

print("approach 1:")
df.withColumn("bonus", df.salary * 0.1).show()

print("approach 2:")
df.withColumn("bonus", expr("salary * 0.1")).show()

print("approach 3:")
df.select(col("name"), col("salary"), (col("salary") * 0.1).alias("bonus")).show()

spark.stop()
