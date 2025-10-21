#!/usr/bin/env python3

import sys
import time
from pyspark import SparkContext

def analyze_dataset(dataset_name, input_file):
    """Simple Spark analysis for one dataset"""
    
    print("========================================")
    print(f"SPARK ANALYSIS: {dataset_name.upper()}")
    print("========================================")
    print(f"Input file: {input_file}")
    print(f"Start time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("")
    
    # Initialize Spark
    sc = SparkContext("spark://spark-master:7077", f"InDegree-{dataset_name}")
    
    try:
        start_time = time.time()
        
        # Load data
        print("Loading data...")
        input_path = f"hdfs://namenode:9000/input/{dataset_name}/{input_file}"
        edges = sc.textFile(input_path)
        
        # Count edges for verification
        total_edges = edges.count()
        print(f"Total edges: {total_edges:,}")
        
        # Calculate in-degrees
        print("Calculating in-degrees...")
        indegree = (
            edges
            .filter(lambda line: line.strip() and not line.startswith('#'))
            .map(lambda line: line.strip().split())
            .filter(lambda parts: len(parts) >= 2)
            .map(lambda parts: (parts[1], 1))
            .reduceByKey(lambda a, b: a + b)
        )
        
        # Calculate distribution
        print("Calculating distribution...")
        distribution = (
            indegree
            .map(lambda node_degree: (node_degree[1], 1))
            .reduceByKey(lambda a, b: a + b)
            .sortByKey()
        )
        
        # Get counts
        unique_nodes = indegree.count()
        distribution_entries = distribution.count()
        
        # Save results
        print("Saving results...")
        output_path = f"hdfs://namenode:9000/output/spark-{dataset_name}"
        
        # Remove existing output
        try:
            import subprocess
            subprocess.run(['hdfs', 'dfs', '-rm', '-r', f'/output/spark-{dataset_name}'], 
                         capture_output=True)
        except:
            pass
            
        distribution.saveAsTextFile(output_path)
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        # Show results
        print("")
        print("=== SPARK RESULTS ===")
        print(f"Dataset: {dataset_name}")
        print(f"Execution time: {execution_time:.2f} seconds")
        print(f"Edges processed: {total_edges:,}")
        print(f"Unique nodes: {unique_nodes:,}")
        print(f"Distribution entries: {distribution_entries}")
        print(f"End time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Show sample distribution
        print("")
        print("Sample distribution (collecting first 10):")
        sample_results = distribution.take(10)
        for degree, count in sample_results:
            print(f"In-degree {degree}: {count} nodes")
        
        print("")
        print(f"SPARK ANALYSIS COMPLETE FOR {dataset_name.upper()}")
        print("========================================")
        
        return execution_time, total_edges, unique_nodes, distribution_entries
        
    except Exception as e:
        print(f"Error: {e}")
        return None
        
    finally:
        sc.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 spark_analysis.py <dataset_name> <input_file>")
        print("Example: python3 spark_analysis.py pokec soc-pokec-relationships.txt")
        sys.exit(1)
    
    dataset = sys.argv[1]
    filename = sys.argv[2]
    
    result = analyze_dataset(dataset, filename)
    if result is None:
        sys.exit(1)