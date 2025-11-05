import sys
import time
import os
from pyspark import SparkContext, SparkConf

def main():
    if len(sys.argv) != 4:
        print("Usage: python3 spark_partitioning_optimized.py <dataset_name> <input_file> <output_dir>")
        sys.exit(1)
    
    dataset_name = sys.argv[1]
    input_file = sys.argv[2]
    output_dir = sys.argv[3]
    
    print("==========================================")
    print(f"SPARK PARTITIONING OPTIMIZATION - {dataset_name.upper()}")
    print("==========================================")
    
    # Optimized Spark configuration - no caching, better partitioning
    app_name = f"PartitionOptimized-{dataset_name}-{int(time.time())}"
    conf = SparkConf().setAppName(app_name)
    conf.setMaster("local[*]")
    
    # Conservative memory settings
    conf.set("spark.executor.memory", "1g")
    conf.set("spark.driver.memory", "512m")
    
    # OPTIMIZATION: Better partitioning and parallelism
    conf.set("spark.default.parallelism", "8")
    conf.set("spark.sql.shuffle.partitions", "8")
    
    conf.set("spark.hadoop.fs.defaultFS", "file:///")
    
    print("=== SPARK OPTIMIZATION: PARTITIONING ===")
    print("Optimized data partitioning for better parallelism")
    print("8 partitions for optimal CPU utilization")
    print("Reduced memory footprint per partition")
    print("========================================")
    print()
    
    input_path = input_file
    
    print(f"Reading from: {input_path}")
    print(f"Output: {output_dir}")
    print(f"Start time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    if not os.path.exists(input_path):
        print(f"ERROR: File {input_path} does not exist!")
        sys.exit(1)
    
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")
    
    print(f"Spark initialized with partitioning optimization")
    print(f"Spark UI: http://localhost:4040")
    print()
    
    try:
        overall_start_time = time.time()
        
        # Job 1: Data Loading with optimal partitioning
        job1_start = time.time()
        print("Loading data with optimal partitioning...")
        
        file_uri = f"file://{os.path.abspath(input_path)}"
        # OPTIMIZATION: Force optimal number of partitions
        edges = sc.textFile(file_uri, minPartitions=16)
        
        job1_time = time.time() - job1_start
        print(f"   Data loaded in {job1_time:.2f} seconds")
        
        # Job 2: Process edges with repartitioning
        job2_start = time.time()
        print("Processing edges with optimal partitioning...")        
        
        processed_edges = (
            edges
            .filter(lambda line: line.strip() and not line.startswith('#'))
            .map(lambda line: line.strip().split())
            .filter(lambda parts: len(parts) >= 2)
            .repartition(8)  # OPTIMIZATION: Optimal partitioning
        )
        
        # Count edges (forces evaluation)
        edge_count = processed_edges.count()
        print(f"   Processed {edge_count:,} edges with 8 partitions")
        
        job2_time = time.time() - job2_start
        print(f"   Edge processing completed in {job2_time:.2f} seconds")
        
        # Job 3: Calculate in-degrees
        job3_start = time.time()
        print("Calculating in-degrees...")
        
        indegree = (
            processed_edges
            .map(lambda parts: (parts[1], 1))
            .reduceByKey(lambda a, b: a + b)
        )
        
        unique_nodes = indegree.count()
        print(f"   Calculated in-degrees for {unique_nodes:,} unique nodes")
        
        job3_time = time.time() - job3_start
        print(f"   In-degrees calculated in {job3_time:.2f} seconds")
        
        # Job 4: Compute distribution
        job4_start = time.time()
        print("Computing distribution...")
        
        distribution = (
            indegree
            .map(lambda node_degree: (node_degree[1], 1))
            .reduceByKey(lambda a, b: a + b)
            .sortByKey()
        )
        
        total_distribution_entries = distribution.count()
        print(f"   Distribution has {total_distribution_entries} unique degree values")
        
        job4_time = time.time() - job4_start
        print(f"   Distribution computed in {job4_time:.2f} seconds")
        
        # Job 5: Save results
        job5_start = time.time()
        print("Saving results...")
        
        try:
            import subprocess
            subprocess.run(['rm', '-rf', output_dir], capture_output=True)
        except:
            pass
        
        output_uri = f"file://{os.path.abspath(output_dir)}"
        distribution.saveAsTextFile(output_uri)
        
        job5_time = time.time() - job5_start
        print(f"   Results saved in {job5_time:.2f} seconds")
        
        overall_execution_time = time.time() - overall_start_time
        
        print("\n" + "="*50)
        print("PARTITIONING OPTIMIZATION RESULTS")
        print("="*50)
        
        print(f"TOTAL EXECUTION TIME: {overall_execution_time:.2f} seconds")
        print(f"Dataset: {dataset_name}")
        print(f"Total Edges: {edge_count:,}")
        print(f"Unique Nodes: {unique_nodes:,}")
        print(f"Distribution Entries: {total_distribution_entries}")
        
        print(f"\nOPTIMIZATION APPLIED:")
        print(f"Partitioning: 16 input partitions → 8 processing partitions")
        print(f"Memory: Conservative allocation (1GB executor, 512MB driver)")
        print(f"Parallelism: 8-way parallel processing")
        print(f"No caching: Avoids memory constraints")
        
        print("\nSample results:")
        sample = distribution.take(5)
        for degree, count in sample:
            print(f"   Degree {degree}: {count:,} nodes")
        
        print(f"\nResults saved to: {output_dir}")
        print(f"Spark UI: http://localhost:4040")
        print("="*50)
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        sc.stop()

if __name__ == "__main__":
    main()