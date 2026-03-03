# spark-submit --master yarn --deploy-mode client shared_folder/query1.py hdfs:///user/cs4433/input/Meta-Event.csv hdfs:///user/cs4433/query1_output
# hdfs dfs -cat hdfs:///user/cs4433/query1_output/part-* | head -n 10

import sys
from pyspark import SparkContext, SparkConf

def run_query1(input_path, output_path):
    # Set up Spark
    conf = SparkConf().setAppName("Project3_Query1")
    sc = SparkContext.getOrCreate(conf=conf)

    # 1. Load the dataset as an RDD
    raw_data = sc.textFile(input_path)

    # 2. Parse and filter using RDD transformations
    sick_people_rdd = raw_data \
        .map(lambda line: line.split(",")) \
        .filter(lambda fields: fields[3] == "sick")

    # 3. Format the output strings
    formatted_results = sick_people_rdd.map(lambda p: f"{p[0]},{p[1]},{p[2]},{p[3]}")

    # 4. Save to the provided output directory argument
    formatted_results.saveAsTextFile(output_path)
    
    print(f"Query 1 Complete. Results saved to: {output_path}")

    sc.stop()

if __name__ == "__main__":
    # Check for both input and output arguments
    if len(sys.argv) < 3:
        print("Usage: spark-submit query1.py <input_path> <output_path>")
        sys.exit(-1)
    
    # sys.argv[1] is input, sys.argv[2] is output
    run_query1(sys.argv[1], sys.argv[2])