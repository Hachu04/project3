from pyspark.sql import SparkSession 

# start spark session 
spark = SparkSession.builder.appName("PurchaseFilter").getOrCreate()

# load purchase.csv file from local storage
purchases = spark.read.csv("Purchases.csv", header=False, inferSchema=True)

purchases = purchases.withColumnRenamed("_c0", "transaction_id") \
                     .withColumnRenamed("_c1", "customer_id") \
                     .withColumnRenamed("_c2", "transaction_total") \
                     .withColumnRenamed("_c3", "num_items") \
                     .withColumnRenamed("_c4", "description")

# register as sql table 
purchases.createOrReplaceTempView("purchases")

# filter out purchases with transtotal > 100
T1 = spark.sql("SELECT * FROM purchases WHERE transaction_total <= 100")

T1.write.csv("T1.csv", header=True, mode="overwrite")

spark.stop()