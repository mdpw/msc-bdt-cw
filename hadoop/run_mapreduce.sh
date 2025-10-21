#!/bin/bash

# Working MapReduce runner for local mode
# Usage: ./run_mapreduce.sh <dataset_name> <input_file>

DATASET=$1
INPUT_FILE=$2

if [ $# -ne 2 ]; then
    echo "Usage: $0 <dataset_name> <input_file>"
    echo "Example: $0 pokec soc-pokec-relationships.txt"
    exit 1
fi

echo "========================================"
echo "HADOOP MAPREDUCE ANALYSIS: $DATASET"
echo "========================================"
echo "Input file: $INPUT_FILE"
echo "Start time: $(date)"
echo ""

# Set working directory
cd /opt/hadoop-code

# Check if JAR file exists, compile if needed
if [ ! -f "indegree-analysis.jar" ]; then
    echo "Compiling Java files..."
    
    export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar
    
    hadoop com.sun.tools.javac.Main *.java
    
    if [ $? -ne 0 ]; then
        echo "ERROR: Compilation failed!"
        exit 1
    fi
    
    jar cf indegree-analysis.jar *.class
    
    # Clean up .class files after JAR creation
    echo "Cleaning up .class files..."
    rm -f *.class
    
    echo "✓ Compilation successful!"
fi

# Check if data file exists
if [ ! -f "/data/$INPUT_FILE" ]; then
    echo "ERROR: Data file /data/$INPUT_FILE not found!"
    echo "Available files in /data/:"
    ls -la /data/
    exit 1
fi

echo "✓ JAR file ready: indegree-analysis.jar"
echo "✓ Data file found: /data/$INPUT_FILE"

# Get file size for reporting
FILE_SIZE=$(ls -lh /data/$INPUT_FILE | awk '{print $5}')
echo "✓ Dataset size: $FILE_SIZE"

# Configure for local mode (no HDFS, no YARN)
unset HADOOP_CONF_DIR
unset CORE_CONF_fs_defaultFS
export MAPREDUCE_FRAMEWORK_NAME=local
export HADOOP_OPTS="-Dfs.defaultFS=file:///"

# Clean previous outputs
echo ""
echo "Cleaning previous outputs..."
rm -rf /tmp/temp-$DATASET
rm -rf /tmp/output-$DATASET

echo "Starting MapReduce job in LOCAL MODE..."
echo "No YARN/HDFS overhead - direct execution"
echo "----------------------------------------"

START_TIME=$(date +%s)

# Run the MapReduce job with local filesystem
hadoop jar indegree-analysis.jar InDegreeDistributionDriver \
    file:///data/$INPUT_FILE \
    file:///tmp/temp-$DATASET \
    file:///tmp/output-$DATASET

# Capture exit status
JOB_STATUS=$?
END_TIME=$(date +%s)
EXECUTION_TIME=$((END_TIME - START_TIME))

echo "----------------------------------------"
echo "Job exit status: $JOB_STATUS"

if [ $JOB_STATUS -eq 0 ]; then
    echo "✓ MapReduce job completed successfully!"
    
    # Check if output exists
    echo ""
    echo "Checking output..."
    ls -la /tmp/output-$DATASET/
    
    if [ -f "/tmp/output-$DATASET/part-r-00000" ]; then
        echo ""
        echo "=== RESULTS ==="
        echo "Sample distribution (first 10):"
        head -10 /tmp/output-$DATASET/part-r-00000
        
        # Count total entries
        TOTAL_ENTRIES=$(wc -l < /tmp/output-$DATASET/part-r-00000)
        echo ""
        echo "Total distribution entries: $TOTAL_ENTRIES"
        
        # Show output size
        OUTPUT_SIZE=$(ls -lh /tmp/output-$DATASET/part-r-00000 | awk '{print $5}')
        echo "Output file size: $OUTPUT_SIZE"
        
        # Show summary statistics
        echo ""
        echo "=== PROCESSING STATISTICS ==="
        echo "Input records processed: $(grep 'Map input records' /dev/null 2>/dev/null || echo 'Check job logs')"
        echo "Output records generated: $TOTAL_ENTRIES unique in-degree values"
        
    else
        echo "WARNING: No output part files found!"
        echo "Output directory contents:"
        ls -la /tmp/output-$DATASET/ || echo "Output directory doesn't exist"
    fi
    
else
    echo "✗ MapReduce job failed with exit code: $JOB_STATUS"
    echo ""
    echo "Check the error messages above for details"
fi

echo ""
echo "=== EXECUTION SUMMARY ==="
echo "Dataset: $DATASET"
echo "Input file: $INPUT_FILE"  
echo "File size: $FILE_SIZE"
echo "Execution time: $EXECUTION_TIME seconds"
echo "Job status: $([ $JOB_STATUS -eq 0 ] && echo 'SUCCESS' || echo 'FAILED')"
echo "Mode: Local MapReduce (no YARN/HDFS overhead)"
echo "End time: $(date)"

echo ""
echo "Output location: /tmp/output-$DATASET/part-r-00000"
echo "To view full results: cat /tmp/output-$DATASET/part-r-00000"
echo ""
echo "========================================"