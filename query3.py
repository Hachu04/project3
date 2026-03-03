# spark-submit --master yarn --deploy-mode client shared_folder/query3.py hdfs:///user/cs4433/input/Meta-Event.csv hdfs:///user/cs4433/query3_output
# hdfs dfs -cat hdfs:///user/cs4433/query3_output/part-* | head -n 10

import sys
from pyspark import SparkContext, SparkConf

def run_query3(input_path, output_path):
    # Set up Spark
    conf = SparkConf().setAppName("Project3_Query3")
    sc = SparkContext.getOrCreate(conf=conf)

    # 1. Load the dataset as an RDD
    raw_data = sc.textFile(input_path)

    # 2. Parse and filter using RDD transformations
    parsed_rdd = raw_data.map(lambda line: line.split(","))
    
    # Get distinct tables which have at least 1 sick person
    sick_table_rdd = parsed_rdd \
        .filter(lambda fields: fields[3] == "sick") \
        .map(lambda fields: (fields[2], fields[3])) \
        .distinct()
    
    # Get list of all healthy people
    healthy_people_rdd = parsed_rdd \
        .filter(lambda fields: fields[3] == "not-sick") \
        .map(lambda fields: (fields[2], fields[0]))

    # 3. Join sick table and healthy people, format
    joined_contacts = healthy_people_rdd.join(sick_table_rdd)
    formatted_results = joined_contacts.map(lambda fields: fields[1][0])

    # 4. Save to the provided output directory argument
    formatted_results.saveAsTextFile(output_path)
    
    print(f"Query 3 Complete. Results saved to: {output_path}")

    sc.stop()

if __name__ == "__main__":
    # Check for both input and output arguments
    if len(sys.argv) < 3:
        print("Usage: spark-submit query3.py <input_path> <output_path>")
        sys.exit(-1)
    
    # sys.argv[1] is input, sys.argv[2] is output
    run_query3(sys.argv[1], sys.argv[2])