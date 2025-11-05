#!/bin/bash

# Simple script to run OPTIMIZED Hadoop MapReduce (separate JAR)
# Usage: ./run_hadoop_optimized.sh <dataset_name> <input_file>

DATASET=$1
INPUT_FILE=$2

if [ $# -ne 2 ]; then
    echo "Usage: $0 <dataset_name> <input_file>"
    echo "Example: $0 livejournal soc-LiveJournal1.txt"
    exit 1
fi

# Set up paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DATA_DIR="$PROJECT_ROOT/data"
HADOOP_DIR="$PROJECT_ROOT/hadoop"
RESULTS_DIR="$PROJECT_ROOT/results/$DATASET/hadoop-optimized"

# Create results directory
mkdir -p "$RESULTS_DIR"

# Create log file with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$RESULTS_DIR/hadoop_optimized_$TIMESTAMP.log"

# Function to log both to console and file
log_both() {
    echo "$1" | tee -a "$LOG_FILE"
}

log_both "========================================"
log_both "HADOOP MAPREDUCE - MEMORY OPTIMIZATION"
log_both "========================================"
log_both "Dataset: $DATASET"
log_both "Input: $INPUT_FILE"
log_both "Start time: $(date)"
log_both "Log file: $LOG_FILE"
log_both ""

# Create hadoop directory if it doesn't exist
mkdir -p "$HADOOP_DIR"
cd "$HADOOP_DIR"

# Copy optimized Java file
cp "$SCRIPT_DIR/InDegreeDistributionDriverOptimized.java" "$HADOOP_DIR/"

# Copy other required Java files (mapper, reducer, etc.)
cp "$PROJECT_ROOT"/*.java "$HADOOP_DIR/" 2>/dev/null || true

OPTIMIZED_JAR="$HADOOP_DIR/indegree-optimized.jar"

# Compile optimized version
log_both "Compiling optimized Hadoop version..."
if [ ! -f "$OPTIMIZED_JAR" ]; then
    export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar
    
    # Compile all Java files
    hadoop com.sun.tools.javac.Main *.java 2>&1 | tee -a "$LOG_FILE"
    
    if [ $? -ne 0 ]; then
        log_both "ERROR: Compilation failed!"
        exit 1
    fi
    
    # Create optimized JAR
    jar cf indegree-optimized.jar *.class 2>&1 | tee -a "$LOG_FILE"
    rm -f *.class
    
    log_both "Optimized JAR created: $OPTIMIZED_JAR"
else
    log_both "Using existing optimized JAR: $OPTIMIZED_JAR"
fi

# Check data file
FULL_INPUT_PATH="$DATA_DIR/$DATASET/$INPUT_FILE"
if [ ! -f "$FULL_INPUT_PATH" ]; then
    log_both "ERROR: Data file not found: $FULL_INPUT_PATH"
    exit 1
fi

FILE_SIZE=$(ls -lh "$FULL_INPUT_PATH" | awk '{print $5}')
FILE_SIZE_BYTES=$(ls -l "$FULL_INPUT_PATH" | awk '{print $5}')
log_both "Data file: $FILE_SIZE ($FILE_SIZE_BYTES bytes)"

# Set up output paths
TEMP_OUTPUT="$RESULTS_DIR/temp-$TIMESTAMP"
FINAL_OUTPUT="$RESULTS_DIR/output-$TIMESTAMP"

mkdir -p "$(dirname "$TEMP_OUTPUT")"
mkdir -p "$(dirname "$FINAL_OUTPUT")"

# Clean previous outputs
rm -rf "$TEMP_OUTPUT"
rm -rf "$FINAL_OUTPUT"

log_both ""
log_both "Running OPTIMIZED Hadoop MapReduce..."
log_both "=== HADOOP OPTIMIZATION: MEMORY TUNING ==="
log_both "Map memory: 3GB (vs 2GB default)"
log_both "Reduce memory: 4GB (vs 2GB default)"
log_both "=========================================="
log_both "Temp: $TEMP_OUTPUT"
log_both "Output: $FINAL_OUTPUT"
log_both "Check YARN UI at http://localhost:8088 for real-time monitoring"
log_both "----------------------------------------"

START_TIME=$(date +%s)

# Run optimized version with full logging
{
    hadoop jar "$OPTIMIZED_JAR" InDegreeDistributionDriverOptimized \
        "file://$FULL_INPUT_PATH" \
        "file://$TEMP_OUTPUT" \
        "file://$FINAL_OUTPUT"
} 2>&1 | tee -a "$LOG_FILE"

EXIT_CODE=${PIPESTATUS[0]}
END_TIME=$(date +%s)
EXECUTION_TIME=$((END_TIME - START_TIME))

log_both "----------------------------------------"
log_both "Job exit status: $EXIT_CODE"

if [ $EXIT_CODE -eq 0 ]; then
    log_both "OPTIMIZED Hadoop completed successfully!"
    log_both "Execution time: $EXECUTION_TIME seconds"
    
    if [ -f "$FINAL_OUTPUT/part-r-00000" ]; then
        log_both ""
        log_both "=== RESULTS ==="
        log_both "Sample distribution (first 10):"
        head -10 "$FINAL_OUTPUT/part-r-00000" | tee -a "$LOG_FILE"
        
        TOTAL_ENTRIES=$(wc -l < "$FINAL_OUTPUT/part-r-00000")
        OUTPUT_SIZE=$(ls -lh "$FINAL_OUTPUT/part-r-00000" | awk '{print $5}')
        
        log_both ""
        log_both "Total distribution entries: $TOTAL_ENTRIES"
        log_both "Output file size: $OUTPUT_SIZE"
        
        log_both ""
        log_both "Top 5 most common in-degrees:"
        sort -k2 -nr "$FINAL_OUTPUT/part-r-00000" | head -5 | tee -a "$LOG_FILE"
        
        log_both ""
        log_both "Full results saved to: $FINAL_OUTPUT/part-r-00000"
    else
        log_both "WARNING: No output part files found!"
    fi
else
    log_both "OPTIMIZED Hadoop failed!"
    log_both "Exit code: $EXIT_CODE"
fi

log_both ""
log_both "=== OPTIMIZED EXECUTION SUMMARY ==="
log_both "Dataset: $DATASET"
log_both "Input file: $INPUT_FILE"
log_both "File size: $FILE_SIZE ($FILE_SIZE_BYTES bytes)"
log_both "Execution time: $EXECUTION_TIME seconds"
log_both "Job status: $([ $EXIT_CODE -eq 0 ] && echo 'SUCCESS' || echo 'FAILED')"
log_both "Mode: OPTIMIZED Local filesystem"
log_both "End time: $(date)"

log_both ""
log_both "OPTIMIZATION SUMMARY:"
log_both "  Applied: Memory tuning only"
log_both "  Map memory: 3GB (vs 2GB default)"
log_both "  Reduce memory: 4GB (vs 2GB default)"
log_both "  JAR file: indegree-optimized.jar"
log_both "  Status: $([ $EXIT_CODE -eq 0 ] && echo 'SUCCESS' || echo 'FAILED')"

log_both ""
log_both "LOG FILES SAVED:"
log_both "  Complete log: $LOG_FILE"
log_both "  Results: $FINAL_OUTPUT/"
log_both "========================================"

echo ""
echo "IMPORTANT: Complete log saved to $LOG_FILE"
echo "Even if terminal gets stuck, check this file for full results!"