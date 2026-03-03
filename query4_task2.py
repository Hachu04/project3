from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("CustomerExpenseCheck").getOrCreate()

# Load T1.csv and Customers.csv
t1 = spark.read.csv("T1.csv", header=True, inferSchema=True)
customers = spark.read.csv("Customers.csv", header=False, inferSchema=True)

customers = customers.withColumnRenamed("_c0", "customer_id") \
                     .withColumnRenamed("_c1", "name") \
                     .withColumnRenamed("_c2", "age") \
                     .withColumnRenamed("_c3", "address") \
                     .withColumnRenamed("_c4", "salary")

# Ensure salary is float
customers = customers.withColumn("salary", f.col("salary").cast("float"))

# Group T1 by customer_id, sum transaction_total, ensure float
t1_grouped = t1.groupBy("customer_id").agg(
    f.sum(f.col("transaction_total").cast("float")).alias("total_expense")
)

# Join with customers to get salary and address
joined = t1_grouped.join(customers, on="customer_id")

# Filter customers who cannot cover their expenses
result = joined.filter(f.col("salary") < f.col("total_expense") * 10)

result.show()

spark.stop()