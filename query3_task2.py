from pyspark.sql import SparkSession
from pyspark.sql import functions as f 

spark = SparkSession.builder.appName("SickPeople").getOrCreate()

t1 = spark.read.csv("T1.csv", header=True, inferSchema=True)
customers = spark.read.csv("Customers.csv", header=False, inferSchema=True)

customers = customers.withColumnRenamed("_c0", "customer_id") \
                     .withColumnRenamed("_c1", "name") \
                     .withColumnRenamed("_c2", "age") \
                     .withColumnRenamed("_c3", "address") \
                     .withColumnRenamed("_c4", "salary")

# join t1 with Customers to get age
joined = t1.join(customers, on="customer_id")

# filter genZ customer 
genz = joined.filter((f.col("age") >= 18) & (f.col("age") <= 21))   

# group by customer_id and age, sum number of items and transaction total 
result = genz.groupBy("customer_id", "age").agg(
    f.sum("num_items").alias("total_num_items"),
    f.sum("transaction_total").alias("total_transaction_total")
)

# save result as t3 csv
result.write.csv("T3.csv", header=True, mode="overwrite")

spark.stop() 
