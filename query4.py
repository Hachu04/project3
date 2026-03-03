# spark-submit --master yarn --deploy-mode client shared_folder/query4.py hdfs:///user/cs4433/input/Meta-Event.csv hdfs:///user/cs4433/query4_output
# hdfs dfs -cat hdfs:///user/cs4433/query4_output/part-* | head -n 10

import sys
from pyspark import SparkContext, SparkConf

def run_query4(input_path, output_path):
    # Set up Spark
    conf = SparkConf().setAppName("Project3_Query4")
    sc = SparkContext.getOrCreate(conf=conf)

    # 1. Load the dataset as an RDD
    raw_data = sc.textFile(input_path)

    # 2. Parse using RDD transformations
    # Map TableID as key, if person is sick then value = (1, 1), else value = (1, 0)
    tables_kv = raw_data \
        .map(lambda line: line.split(",")) \
        .map(lambda fields: (fields[2], (1,1 if fields[3] == "sick" else 0)))
    
    # Use reduceByKey to aggregate count and sick flag
    aggregated_tables = tables_kv.reduceByKey(
        lambda current_total, new_record: (
            current_total[0] + new_record[0],  # Summing the people count
            current_total[1] + new_record[1]   # Summing the sick flag
        )
    )

    # 3. Format the output strings
    # x is (TableID, (TotalCount, SickFlag))
    formatted_results = aggregated_tables.map(lambda x: f"{x[0]},{x[1][0]}," + ("concern" if x[1][1] > 0 else "healthy"))

    # 4. Save to the provided output directory argument
    formatted_results.saveAsTextFile(output_path)
    
    print(f"Query 4 Complete. Results saved to: {output_path}")

    sc.stop()

if __name__ == "__main__":
    # Check for both input and output arguments
    if len(sys.argv) < 3:
        print("Usage: spark-submit query4.py <input_path> <output_path>")
        sys.exit(-1)
    
    # sys.argv[1] is input, sys.argv[2] is output
    run_query4(sys.argv[1], sys.argv[2])