#!/bin/bash

# Universal MapReduce runner for in-degree analysis
# Usage: ./run_mapreduce.sh <dataset_name> <input_file>

DATASET=$1
INPUT_FILE=$2

if [ $# -ne 2 ]; then
    echo "Usage: $0 <dataset_name> <input_file>"
    echo "Example: $0 pokec soc-pokec-relationships.txt"
    exit 1
fi

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Set paths relative to project structure
DATA_DIR="$PROJECT_ROOT/data"
HADOOP_DIR="$PROJECT_ROOT/hadoop"
JAR_FILE="$HADOOP_DIR/indegree-analysis.jar"
RESULTS_DIR="$PROJECT_ROOT/results/$DATASET/hadoop"

# Create results directory
mkdir -p "$RESULTS_DIR"

# Create log file with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$RESULTS_DIR/hadoop_baseline_$TIMESTAMP.log"
TEMP_OUTPUT="$RESULTS_DIR/temp-$TIMESTAMP"
FINAL_OUTPUT="$RESULTS_DIR/output-$TIMESTAMP"

# Function to log both to console and file
log_both() {
    echo "$1" | tee -a "$LOG_FILE"
}

log_both "========================================"
log_both "HADOOP MAPREDUCE ANALYSIS: $DATASET"
log_both "========================================"
log_both "Input file: $INPUT_FILE"
log_both "Start time: $(date)"
log_both "Log file: $LOG_FILE"
log_both ""

log_both "Project root: $PROJECT_ROOT"
log_both "Data directory: $DATA_DIR"
log_both "Hadoop directory: $HADOOP_DIR"
log_both "Temp output: $TEMP_OUTPUT"
log_both "Final output: $FINAL_OUTPUT"
log_both ""

# Change to hadoop directory for compilation if needed
cd "$HADOOP_DIR"

# Check if JAR file exists, compile if needed
if [ ! -f "$JAR_FILE" ]; then
    log_both "JAR file not found. Compiling Java files..."
    
    # Check if Java source files exist
    if ls *.java 1> /dev/null 2>&1; then
        export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar
        
        # Compile Java files
        hadoop com.sun.tools.javac.Main *.java 2>&1 | tee -a "$LOG_FILE"
        
        if [ $? -ne 0 ]; then
            log_both "ERROR: Compilation failed!"
            exit 1
        fi
        
        # Create JAR file
        jar cf indegree-analysis.jar *.class 2>&1 | tee -a "$LOG_FILE"
        
        # Clean up .class files after JAR creation
        log_both "Cleaning up .class files..."
        rm -f *.class
        
        log_both "Compilation successful!"
    else
        log_both "ERROR: No Java source files found in $HADOOP_DIR"
        log_both "Available files:"
        ls -la "$HADOOP_DIR" | tee -a "$LOG_FILE"
        exit 1
    fi
fi

# Check if data file exists
FULL_INPUT_PATH="$DATA_DIR/$DATASET/$INPUT_FILE"
if [ ! -f "$FULL_INPUT_PATH" ]; then
    log_both "ERROR: Data file $FULL_INPUT_PATH not found!"
    log_both "Available files in $DATA_DIR/$DATASET:"
    ls -la "$DATA_DIR/$DATASET" 2>/dev/null | tee -a "$LOG_FILE" || log_both "Directory does not exist"
    exit 1
fi

log_both "JAR file ready: $JAR_FILE"
log_both "Data file found: $FULL_INPUT_PATH"

# Get file size for reporting
FILE_SIZE=$(ls -lh "$FULL_INPUT_PATH" | awk '{print $5}')
FILE_SIZE_BYTES=$(ls -l "$FULL_INPUT_PATH" | awk '{print $5}')
log_both "Dataset: $DATASET"
log_both "Dataset size: $FILE_SIZE ($FILE_SIZE_BYTES bytes)"

# Create output directories
mkdir -p "$(dirname "$TEMP_OUTPUT")"
mkdir -p "$(dirname "$FINAL_OUTPUT")"

# Clean previous outputs
log_both ""
log_both "Cleaning previous outputs..."
rm -rf "$TEMP_OUTPUT"
rm -rf "$FINAL_OUTPUT"

# Configure for local mode (no HDFS, no YARN) or use HDFS
if hadoop fs -ls / &> /dev/null; then
    log_both "Using HDFS mode..."
    
    # Use HDFS paths
    HDFS_INPUT="/input/$DATASET/$INPUT_FILE"
    HDFS_TEMP="/temp/$DATASET-$TIMESTAMP"
    HDFS_OUTPUT="/output/$DATASET-$TIMESTAMP"
    
    # Copy input to HDFS if not exists
    if ! hadoop fs -test -e "$HDFS_INPUT"; then
        log_both "Copying input file to HDFS..."
        hadoop fs -mkdir -p "/input/$DATASET" 2>&1 | tee -a "$LOG_FILE"
        hadoop fs -put "$FULL_INPUT_PATH" "$HDFS_INPUT" 2>&1 | tee -a "$LOG_FILE"
    fi
    
    # Clean HDFS outputs
    hadoop fs -rm -r "$HDFS_TEMP" 2>/dev/null || true
    hadoop fs -rm -r "$HDFS_OUTPUT" 2>/dev/null || true
    
    INPUT_PATH="$HDFS_INPUT"
    TEMP_PATH="$HDFS_TEMP"
    OUTPUT_PATH="$HDFS_OUTPUT"
    
else
    log_both "Using local filesystem mode..."
    
    # Configure for local mode
    unset HADOOP_CONF_DIR
    unset CORE_CONF_fs_defaultFS
    export MAPREDUCE_FRAMEWORK_NAME=local
    export HADOOP_OPTS="-Dfs.defaultFS=file:///"
    
    INPUT_PATH="file://$FULL_INPUT_PATH"
    TEMP_PATH="file://$TEMP_OUTPUT"
    OUTPUT_PATH="file://$FINAL_OUTPUT"
fi

log_both "Starting MapReduce job..."
log_both "Check YARN UI at http://localhost:8088 for real-time monitoring"
log_both "Input: $INPUT_PATH"
log_both "Temp: $TEMP_PATH"
log_both "Output: $OUTPUT_PATH"
log_both "----------------------------------------"

START_TIME=$(date +%s)

# Run the MapReduce job with full logging
{
    hadoop jar "$JAR_FILE" InDegreeDistributionDriver \
        "$INPUT_PATH" \
        "$TEMP_PATH" \
        "$OUTPUT_PATH"
} 2>&1 | tee -a "$LOG_FILE"

# Capture exit status
JOB_STATUS=${PIPESTATUS[0]}
END_TIME=$(date +%s)
EXECUTION_TIME=$((END_TIME - START_TIME))

log_both "----------------------------------------"
log_both "Job exit status: $JOB_STATUS"

if [ $JOB_STATUS -eq 0 ]; then
    log_both "MapReduce job completed successfully!"
    
    # Copy results from HDFS to local if needed
    if hadoop fs -ls / &> /dev/null; then
        log_both "Copying results from HDFS to local filesystem..."
        hadoop fs -get "$OUTPUT_PATH" "$FINAL_OUTPUT" 2>&1 | tee -a "$LOG_FILE"
    fi
    
    # Check if output exists
    log_both ""
    log_both "Checking output..."
    ls -la "$FINAL_OUTPUT"/ 2>/dev/null | tee -a "$LOG_FILE"
    
    if [ -f "$FINAL_OUTPUT/part-r-00000" ]; then
        log_both ""
        log_both "=== RESULTS ==="
        log_both "Sample distribution (first 10):"
        head -10 "$FINAL_OUTPUT/part-r-00000" | tee -a "$LOG_FILE"
        
        # Count total entries
        TOTAL_ENTRIES=$(wc -l < "$FINAL_OUTPUT/part-r-00000")
        log_both ""
        log_both "Total distribution entries: $TOTAL_ENTRIES"
        
        # Show output size
        OUTPUT_SIZE=$(ls -lh "$FINAL_OUTPUT/part-r-00000" | awk '{print $5}')
        log_both "Output file size: $OUTPUT_SIZE"
        
        # Show top 5 most common in-degrees
        log_both ""
        log_both "Top 5 most common in-degrees:"
        sort -k2 -nr "$FINAL_OUTPUT/part-r-00000" | head -5 | tee -a "$LOG_FILE"
        
    else
        log_both "WARNING: No output part files found!"
        log_both "Output directory contents:"
        ls -la "$FINAL_OUTPUT"/ | tee -a "$LOG_FILE" || log_both "Output directory doesn't exist"
    fi
    
else
    log_both "MapReduce job failed with exit code: $JOB_STATUS"
    log_both ""
    log_both "Check the error messages above for details"
    log_both "Common issues:"
    log_both "- Java heap space (try: export HADOOP_OPTS='-Xmx2g')"
    log_both "- Input file format issues"
    log_both "- JAR file class path problems"
    log_both "- HDFS permissions"
fi

log_both ""
log_both "=== EXECUTION SUMMARY ==="
log_both "Dataset: $DATASET"
log_both "Input file: $INPUT_FILE"
log_both "File size: $FILE_SIZE ($FILE_SIZE_BYTES bytes)"
log_both "Execution time: $EXECUTION_TIME seconds"
log_both "Job status: $([ $JOB_STATUS -eq 0 ] && echo 'SUCCESS' || echo 'FAILED')"
log_both "Mode: $([ -n "$HDFS_INPUT" ] && echo 'HDFS' || echo 'Local filesystem')"
log_both "End time: $(date)"

log_both ""
log_both "Output location: $FINAL_OUTPUT/part-r-00000"
log_both "To view full results: cat $FINAL_OUTPUT/part-r-00000"

log_both ""
log_both "LOG FILES SAVED:"
log_both "  Complete log: $LOG_FILE"
log_both "  Results: $FINAL_OUTPUT/"
log_both "========================================"

echo ""
echo "IMPORTANT: Complete log saved to $LOG_FILE"
echo "Even if terminal gets stuck, check this file for full results!"