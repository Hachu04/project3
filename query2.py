# spark-submit --master yarn --deploy-mode client shared_folder/query2.py hdfs:///user/cs4433/input/Meta-Event-No-Disclosure.csv hdfs:///user/cs4433/input/Reported-Illnesses.csv hdfs:///user/cs4433/query2_output
# hdfs dfs -cat hdfs:///user/cs4433/query2_output/part-* | head -n 10

import sys
from pyspark import SparkContext, SparkConf

def run_query2(meta_event, illnesses, output_path):
    # Set up Spark
    conf = SparkConf().setAppName("Project3_Query2")
    sc = SparkContext.getOrCreate(conf=conf)

    # 1. Load the dataset as an RDD
    raw_meta = sc.textFile(meta_event)
    raw_illnesses = sc.textFile(illnesses)
    
    # 2. Parse using RDD transformations
    meta_kv = raw_meta.map(lambda line: line.split(",")) \
                    .map(lambda fields: (fields[0], fields[2]))
                
    illness_kv = raw_illnesses.map(lambda line: line.split(","))

    # 3. Perform the Join
    joined_rdd = meta_kv.join(illness_kv)
    
    formatted_results = joined_rdd.map(lambda x: f"{x[0]},{x[1][0]}")

    # 4. Save to the provided output directory argument
    formatted_results.saveAsTextFile(output_path)
    
    print(f"Query 2 Complete. Results saved to: {output_path}")

    sc.stop()

if __name__ == "__main__":
    # Check for 2 input and 1 output
    if len(sys.argv) < 4:
        print("Usage: spark-submit query2.py <Meta-Event-No-Disclosure_path> <Reported-Illnesses_path> <output_path>")
        sys.exit(-1)
    
    # sys.argv[1] is Meta-Event-No-Disclosure, sys.argv[2] is Reported-Illnesses, sys.argv[3] is output
    run_query2(sys.argv[1], sys.argv[2], sys.argv[3])