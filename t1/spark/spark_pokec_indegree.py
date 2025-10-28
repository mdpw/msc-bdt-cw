#!/usr/bin/env python3

import sys
import time
from pyspark import SparkContext, SparkConf

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 spark_pokec_indegree.py <input_file> <output_dir>")
        print("Example: python3 spark_pokec_indegree.py /data/pokec/soc-pokec-relationships.txt /output/spark-pokec")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_dir = sys.argv[2]
    
    print("==========================================")
    print("SPARK IN-DEGREE ANALYSIS - POKEC DATASET")
    print("==========================================")
    print(f"Input: {input_file}")
    print(f"Output: {output_dir}")
    print(f"Start time: {time.strftime('%H:%M:%S')}")
    print()
    
    # Configure Spark for standalone mode
    conf = SparkConf().setAppName("PokecInDegreeAnalysis")
    conf.setMaster("local[*]")  # Use all available cores
    conf.set("spark.executor.memory", "2g")
    
    # Initialize Spark
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")  # Reduce log output
    
    try:
        start_time = time.time()
        
        # Load data
        print("Loading data...")
        edges = sc.textFile(input_file)
        
        # Process edges and calculate in-degrees
        print("Calculating in-degrees...")
        indegree = (
            edges
            .filter(lambda line: line.strip() and not line.startswith('#'))
            .map(lambda line: line.strip().split())
            .filter(lambda parts: len(parts) >= 2)
            .map(lambda parts: (parts[1], 1))  # destination node gets +1 indegree
            .reduceByKey(lambda a, b: a + b)
        )
        
        # Calculate distribution
        print("Computing distribution...")
        distribution = (
            indegree
            .map(lambda node_degree: (node_degree[1], 1))  # (degree, count)
            .reduceByKey(lambda a, b: a + b)
            .sortByKey()
        )
        
        # Get statistics
        total_edges = edges.filter(lambda line: line.strip() and not line.startswith('#')).count()
        unique_nodes = indegree.count()
        
        # Save results
        print("Saving results...")
        distribution.saveAsTextFile(output_dir)
        
        # Show results
        execution_time = time.time() - start_time
        
        print("\n=== RESULTS ===")
        print(f"Total edges: {total_edges:,}")
        print(f"Nodes with in-degree > 0: {unique_nodes:,}")
        print(f"Execution time: {execution_time:.2f} seconds")
        
        # Display sample distribution
        print("\nIn-degree distribution (first 10):")
        sample = distribution.take(10)
        for degree, count in sample:
            print(f"  Degree {degree}: {count} nodes")
        
        print(f"\nResults saved to: {output_dir}")
        print("==========================================")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
        
    finally:
        sc.stop()

if __name__ == "__main__":
    main()