import sys
import time
import os
import psutil
from pyspark import SparkContext, SparkConf

def format_bytes(bytes_value):
    """Format bytes to human readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.2f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.2f} PB"

def format_time(seconds):
    """Format seconds to human readable format"""
    if seconds < 60:
        return f"{seconds:.2f} seconds"
    elif seconds < 3600:
        minutes = seconds // 60
        secs = seconds % 60
        return f"{int(minutes)}m {secs:.2f}s"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        return f"{int(hours)}h {int(minutes)}m {secs:.2f}s"

def get_system_metrics():
    """Get current system metrics"""
    try:
        process = psutil.Process()
        memory_info = process.memory_info()
        return {
            'cpu_percent': psutil.cpu_percent(),
            'memory_rss': memory_info.rss,
            'memory_vms': memory_info.vms,
            'memory_percent': process.memory_percent(),
            'total_system_memory': psutil.virtual_memory().total,
            'available_system_memory': psutil.virtual_memory().available
        }
    except:
        return {}

def main():
    if len(sys.argv) != 4:
        print("Usage: python3 spark_indegree_analysis.py <dataset_name> <input_file> <output_dir>")
        sys.exit(1)
    
    dataset_name = sys.argv[1]
    input_file = sys.argv[2]
    output_dir = sys.argv[3]
    
    print("==========================================")
    print(f"SPARK IN-DEGREE ANALYSIS - {dataset_name.upper()} DATASET")
    print("==========================================")
    
    # Force local mode and use local file path
    app_name = f"InDegreeAnalysis-{dataset_name}-{int(time.time())}"
    conf = SparkConf().setAppName(app_name)
    conf.setMaster("local[*]")
    conf.set("spark.executor.memory", "2g")
    conf.set("spark.driver.memory", "1g")
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.dir", "/tmp/spark-events")
    conf.set("spark.ui.enabled", "true")
    conf.set("spark.ui.port", "4040")
    
    # CRITICAL: Force local filesystem to avoid HDFS issues
    conf.set("spark.hadoop.fs.defaultFS", "file:///")
    
    # Use local file path directly
    input_path = input_file
    
    print(f"Reading from: {input_path}")
    print(f"Output: {output_dir}")
    print(f"Start time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Verify file exists
    if not os.path.exists(input_path):
        print(f"ERROR: File {input_path} does not exist!")
        sys.exit(1)
    
    print(f"Input file verified: {format_bytes(os.path.getsize(input_path))}")
    
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")
    
    print(f"Spark initialized in LOCAL mode")
    print(f"Spark UI: http://localhost:4040")
    print(f"Application ID: {sc.applicationId}")
    print()
    
    try:
        overall_start_time = time.time()
        
        # Job 1: Data Loading
        job1_start = time.time()
        print("Loading data...")
        
        # Use file:// protocol to force local filesystem
        file_uri = f"file://{os.path.abspath(input_path)}"
        edges = sc.textFile(file_uri)
        
        job1_time = time.time() - job1_start
        print(f"   Data loaded in {format_time(job1_time)}")
        
        # Job 2: Calculate in-degrees
        job2_start = time.time()
        print("Calculating in-degrees...")        
        
        # Process edges exactly like Hadoop Mapper
        processed_edges = (
            edges
            .filter(lambda line: line.strip() and not line.startswith('#'))
            .map(lambda line: line.strip().split())
            .filter(lambda parts: len(parts) >= 2)
            # NO numeric filtering - accept all valid node IDs (matches corrected Java code)
        )
        
        # Calculate in-degrees
        indegree = (
            processed_edges
            .map(lambda parts: (parts[1], 1))  # Target node gets +1 in-degree
            .reduceByKey(lambda a, b: a + b)
        )
        
        job2_time = time.time() - job2_start
        print(f"   In-degrees calculated in {format_time(job2_time)}")
        
        # Job 3: Compute distribution
        job3_start = time.time()
        print("Computing distribution...")
        
        distribution = (
            indegree
            .map(lambda node_degree: (node_degree[1], 1))
            .reduceByKey(lambda a, b: a + b)
            .sortByKey()
        )
        
        job3_time = time.time() - job3_start
        print(f"   Distribution computed in {format_time(job3_time)}")
        
        # Job 4: Calculate statistics
        job4_start = time.time()
        print("Calculating statistics...")
        
        # Count processed edges (matches what Hadoop actually processes)
        total_edges = processed_edges.count()
        unique_nodes = indegree.count()
        total_distribution_entries = distribution.count()
        
        job4_time = time.time() - job4_start
        print(f"   Statistics calculated in {format_time(job4_time)}")
        
        # Job 5: Save results
        job5_start = time.time()
        print("Saving results...")
        
        try:
            import subprocess
            subprocess.run(['rm', '-rf', output_dir], capture_output=True)
        except:
            pass
        
        # Use file:// protocol for output as well
        output_uri = f"file://{os.path.abspath(output_dir)}"
        distribution.saveAsTextFile(output_uri)
        
        job5_time = time.time() - job5_start
        print(f"   Results saved in {format_time(job5_time)}")
        
        overall_execution_time = time.time() - overall_start_time
        
        # Get system metrics
        sys_metrics = get_system_metrics()
        
        print("\n" + "="*50)
        print("COMPREHENSIVE PERFORMANCE METRICS")
        print("="*50)
        
        print(f"\nEXECUTION TIME BREAKDOWN:")
        print(f"   Job 1 (Data Loading): {format_time(job1_time)} ({job1_time*1000:.0f} ms)")
        print(f"   Job 2 (Calculate In-Degrees): {format_time(job2_time)} ({job2_time*1000:.0f} ms)")
        print(f"   Job 3 (Calculate Distribution): {format_time(job3_time)} ({job3_time*1000:.0f} ms)")
        print(f"   Job 4 (Calculate Statistics): {format_time(job4_time)} ({job4_time*1000:.0f} ms)")
        print(f"   Job 5 (Save Results): {format_time(job5_time)} ({job5_time*1000:.0f} ms)")
        print(f"   TOTAL EXECUTION TIME: {format_time(overall_execution_time)} ({overall_execution_time*1000:.0f} ms)")
        
        print(f"\nSYSTEM RESOURCE METRICS:")
        if sys_metrics:
            print(f"   CPU Usage: {sys_metrics.get('cpu_percent', 0):.1f}%")
            print(f"   Process Memory (RSS): {format_bytes(sys_metrics.get('memory_rss', 0))}")
            print(f"   Process Memory (VMS): {format_bytes(sys_metrics.get('memory_vms', 0))}")
            print(f"   Memory Usage: {sys_metrics.get('memory_percent', 0):.1f}%")
            print(f"   Total System Memory: {format_bytes(sys_metrics.get('total_system_memory', 0))}")
            print(f"   Available System Memory: {format_bytes(sys_metrics.get('available_system_memory', 0))}")
        
        print(f"\nDATA PROCESSING STATISTICS:")
        print(f"   Dataset: {dataset_name}")
        print(f"   Input File Size: {format_bytes(os.path.getsize(input_path))}")
        print(f"   Total Edges Processed: {total_edges:,}")
        print(f"   Unique Nodes: {unique_nodes:,}")
        print(f"   Distribution Entries: {total_distribution_entries}")
        print(f"   Application ID: {sc.applicationId}")
        
        print("\nIn-degree distribution (first 10):")
        sample = distribution.take(10)
        for degree, count in sample:
            print(f"   Degree {degree}: {count:,} nodes")
        
        print("\nTop 5 highest in-degrees:")
        top_nodes = indegree.top(5, key=lambda x: x[1])
        for node_id, degree in top_nodes:
            print(f"   Node {node_id}: {degree:,} in-degree")
        
        # Save comprehensive summary
        summary_file = f"{output_dir}_comprehensive_summary.txt"
        with open(summary_file, "w") as f:
            f.write(f"SPARK IN-DEGREE ANALYSIS - COMPREHENSIVE SUMMARY\n")
            f.write(f"================================================\n")
            f.write(f"Execution Mode: LOCAL\n")
            f.write(f"Dataset: {dataset_name}\n")
            f.write(f"Application: {app_name}\n")
            f.write(f"Application ID: {sc.applicationId}\n")
            f.write(f"Input File: {input_file}\n")
            f.write(f"Input File Size: {format_bytes(os.path.getsize(input_path))}\n")
            f.write(f"Output Directory: {output_dir}\n")
            f.write(f"Start Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(overall_start_time))}\n")
            f.write(f"\nEXECUTION TIME BREAKDOWN:\n")
            f.write(f"Job 1 (Data Loading): {format_time(job1_time)} ({job1_time*1000:.0f} ms)\n")
            f.write(f"Job 2 (Calculate In-Degrees): {format_time(job2_time)} ({job2_time*1000:.0f} ms)\n")
            f.write(f"Job 3 (Calculate Distribution): {format_time(job3_time)} ({job3_time*1000:.0f} ms)\n")
            f.write(f"Job 4 (Calculate Statistics): {format_time(job4_time)} ({job4_time*1000:.0f} ms)\n")
            f.write(f"Job 5 (Save Results): {format_time(job5_time)} ({job5_time*1000:.0f} ms)\n")
            f.write(f"TOTAL EXECUTION TIME: {format_time(overall_execution_time)} ({overall_execution_time*1000:.0f} ms)\n")
            f.write(f"\nDATA PROCESSING RESULTS:\n")
            f.write(f"Total Edges: {total_edges:,}\n")
            f.write(f"Unique Nodes: {unique_nodes:,}\n")
            f.write(f"Distribution Entries: {total_distribution_entries}\n")
            if sys_metrics:
                f.write(f"\nSYSTEM METRICS:\n")
                f.write(f"Process Memory (RSS): {format_bytes(sys_metrics.get('memory_rss', 0))}\n")
                f.write(f"Process Memory (VMS): {format_bytes(sys_metrics.get('memory_vms', 0))}\n")
                f.write(f"Total System Memory: {format_bytes(sys_metrics.get('total_system_memory', 0))}\n")
        
        print(f"\nResults saved to: {output_dir}")
        print(f"Comprehensive summary: {summary_file}")
        
        print(f"\nSPARK UI METRICS AVAILABLE:")
        print(f"   Live Spark UI: http://localhost:4040")
        print(f"   Spark History: http://localhost:18080 (after completion)")
        print(f"   Detailed job/stage/task metrics available in UI")
        
        print(f"\nFOR HADOOP COMPARISON:")
        print(f"   Total Execution Time: {format_time(overall_execution_time)} ({overall_execution_time*1000:.0f} ms)")
        print(f"   Job-by-job breakdown matches Hadoop MapReduce structure")
        print(f"   Take screenshots of Spark UI for side-by-side comparison")
        print("="*50)
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        sc.stop()

if __name__ == "__main__":
    main()