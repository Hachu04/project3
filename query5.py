# spark-submit --master yarn --deploy-mode client shared_folder/query5.py hdfs:///user/cs4433/input/Meta-Event-No-Disclosure.csv hdfs:///user/cs4433/input/Reported-Illnesses.csv hdfs:///user/cs4433/query5_output
# hdfs dfs -cat hdfs:///user/cs4433/query5_output/part-* | head -n 10

import sys
from pyspark import SparkContext, SparkConf

def run_query5(meta_event, illnesses, output_path):
    # Set up Spark
    conf = SparkConf().setAppName("Project3_Query5")
    sc = SparkContext.getOrCreate(conf=conf)

    # 1. Load the dataset as an RDD
    raw_meta = sc.textFile(meta_event)
    raw_illnesses = sc.textFile(illnesses)
    
    parsed_meta = raw_meta.map(lambda line: line.split(","))
    
    # 2. First join to get sick tables
    meta_kv = parsed_meta.map(lambda fields: (fields[0], fields[2]))
                
    illness_kv = raw_illnesses.map(lambda line: line.split(","))

    sick_tables = meta_kv.join(illness_kv)
    unique_sick_tables = sick_tables.map(lambda x: (x[1][0], 1)).distinct()
    
    # 3. Second join to get healthy people in sick tables
    person_kv = parsed_meta.map(lambda fields: (fields[2], fields[0]))
    sick_ids = raw_illnesses.map(lambda line: line.split(",")[0])
    
    joined_all = person_kv.join(unique_sick_tables)
    
    formatted_results = joined_all.map(lambda x: x[1][0]).subtract(sick_ids)
    
    # 4. Save to the provided output directory argument
    formatted_results.saveAsTextFile(output_path)
    
    print(f"Query 5 Complete. Results saved to: {output_path}")

    sc.stop()

if __name__ == "__main__":
    # Check for 2 input and 1 output
    if len(sys.argv) < 4:
        print("Usage: spark-submit query5.py <Meta-Event-No-Disclosure_path> <Reported-Illnesses_path> <output_path>")
        sys.exit(-1)
    
    # sys.argv[1] is Meta-Event-No-Disclosure, sys.argv[2] is Reported-Illnesses, sys.argv[3] is output
    run_query5(sys.argv[1], sys.argv[2], sys.argv[3])