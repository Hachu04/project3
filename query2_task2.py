from pyspark.sql import SparkSession 
from pyspark.sql import functions as f
from pyspark.sql.window import Window 

spark = SparkSession.builder.appName("PurchasesStats").getOrCreate()

# load t1.csv
t1 = spark.read.csv("T1.csv", header=True, inferSchema=True)

# group by num_items and calculate min, max, and median of transaction_total 
# median calculation using percentile_approx
result = t1.groupBy("num_items").agg(
    f.min("transaction_total").alias("min_transaction_total"),
    f.max("transaction_total").alias("max_transaction_total"),
    f.expr("percentile_approx(transaction_total, 0.5)").alias("median_transaction_total")
)

# show result to client side
result.show() 

spark.stop()
