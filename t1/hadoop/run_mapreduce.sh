#!/bin/bash

# Ubuntu-compatible MapReduce runner for local mode
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

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Set paths relative to project structure
DATA_DIR="$PROJECT_ROOT/data"
HADOOP_DIR="$PROJECT_ROOT/hadoop"
JAR_FILE="$HADOOP_DIR/indegree-analysis.jar"

echo "Project root: $PROJECT_ROOT"
echo "Data directory: $DATA_DIR"
echo "Hadoop directory: $HADOOP_DIR"
echo ""

# Change to hadoop directory for compilation if needed
cd "$HADOOP_DIR"

# Check if JAR file exists, compile if needed
if [ ! -f "$JAR_FILE" ]; then
    echo "JAR file not found. Compiling Java files..."
    
    # Check if Java source files exist
    if ls *.java 1> /dev/null 2>&1; then
        export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar
        
        # Compile Java files
        hadoop com.sun.tools.javac.Main *.java
        
        if [ $? -ne 0 ]; then
            echo "ERROR: Compilation failed!"
            exit 1
        fi
        
        # Create JAR file
        jar cf indegree-analysis.jar *.class
        
        # Clean up .class files after JAR creation
        echo "Cleaning up .class files..."
        rm -f *.class
        
        echo "✓ Compilation successful!"
    else
        echo "ERROR: No Java source files found in $HADOOP_DIR"
        echo "Available files:"
        ls -la "$HADOOP_DIR"
        exit 1
    fi
fi

# Check if data file exists
FULL_INPUT_PATH="$DATA_DIR/$INPUT_FILE"
if [ ! -f "$FULL_INPUT_PATH" ]; then
    echo "ERROR: Data file $FULL_INPUT_PATH not found!"
    echo "Available files in $DATA_DIR:"
    ls -la "$DATA_DIR"
    exit 1
fi

echo "✓ JAR file ready: $JAR_FILE"
echo "✓ Data file found: $FULL_INPUT_PATH"

# Get file size for reporting
FILE_SIZE=$(ls -lh "$FULL_INPUT_PATH" | awk '{print $5}')
echo "✓ Dataset size: $FILE_SIZE"

# Configure for local mode (no HDFS, no YARN)
unset HADOOP_CONF_DIR
unset CORE_CONF_fs_defaultFS
export MAPREDUCE_FRAMEWORK_NAME=local
export HADOOP_OPTS="-Dfs.defaultFS=file:///"

# Set output directories in /tmp
TEMP_OUTPUT="/tmp/temp-$DATASET"
FINAL_OUTPUT="/tmp/output-$DATASET"

# Clean previous outputs
echo ""
echo "Cleaning previous outputs..."
rm -rf "$TEMP_OUTPUT"
rm -rf "$FINAL_OUTPUT"

echo "Starting MapReduce job in LOCAL MODE..."
echo "No YARN/HDFS overhead - direct execution"
echo "Input: $FULL_INPUT_PATH"
echo "Output: $FINAL_OUTPUT"
echo "----------------------------------------"

START_TIME=$(date +%s)

# Run the MapReduce job with local filesystem
hadoop jar "$JAR_FILE" InDegreeDistributionDriver \
    "file://$FULL_INPUT_PATH" \
    "file://$TEMP_OUTPUT" \
    "file://$FINAL_OUTPUT"

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
    ls -la "$FINAL_OUTPUT"/
    
    if [ -f "$FINAL_OUTPUT/part-r-00000" ]; then
        echo ""
        echo "=== RESULTS ==="
        echo "Sample distribution (first 10):"
        head -10 "$FINAL_OUTPUT/part-r-00000"
        
        # Count total entries
        TOTAL_ENTRIES=$(wc -l < "$FINAL_OUTPUT/part-r-00000")
        echo ""
        echo "Total distribution entries: $TOTAL_ENTRIES"
        
        # Show output size
        OUTPUT_SIZE=$(ls -lh "$FINAL_OUTPUT/part-r-00000" | awk '{print $5}')
        echo "Output file size: $OUTPUT_SIZE"
        
        # Show summary statistics
        echo ""
        echo "=== PROCESSING STATISTICS ==="
        echo "Output records generated: $TOTAL_ENTRIES unique in-degree values"
        
        # Show top 5 most common in-degrees
        echo ""
        echo "Top 5 most common in-degrees:"
        sort -k2 -nr "$FINAL_OUTPUT/part-r-00000" | head -5
        
    else
        echo "WARNING: No output part files found!"
        echo "Output directory contents:"
        ls -la "$FINAL_OUTPUT"/ || echo "Output directory doesn't exist"
    fi
    
else
    echo "✗ MapReduce job failed with exit code: $JOB_STATUS"
    echo ""
    echo "Check the error messages above for details"
    echo "Common issues:"
    echo "- Java heap space (try: export HADOOP_OPTS='-Xmx2g')"
    echo "- Input file format issues"
    echo "- JAR file class path problems"
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
echo "Output location: $FINAL_OUTPUT/part-r-00000"
echo "To view full results: cat $FINAL_OUTPUT/part-r-00000"
echo ""
echo "========================================"