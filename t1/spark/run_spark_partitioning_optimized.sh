#!/bin/bash

# Simple script to run PARTITIONING OPTIMIZED Spark
# Usage: ./run_spark_partitioning_optimized.sh <dataset_name> <input_file>

DATASET=$1
INPUT_FILE=$2

if [ $# -ne 2 ]; then
    echo "Usage: $0 <dataset_name> <input_file>"
    echo "Example: $0 live-journal soc-LiveJournal1.txt"
    exit 1
fi

# Set up paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DATA_DIR="$PROJECT_ROOT/data"
RESULTS_DIR="$PROJECT_ROOT/results/$DATASET/spark-partitioning-optimized"

# Create results directory
mkdir -p "$RESULTS_DIR"

# Create log file with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$RESULTS_DIR/spark_partitioning_$TIMESTAMP.log"
OUTPUT_DIR="$RESULTS_DIR/output-$TIMESTAMP"

# Function to log both to console and file
log_both() {
    echo "$1" | tee -a "$LOG_FILE"
}

log_both "========================================"
log_both "SPARK - PARTITIONING OPTIMIZATION"
log_both "========================================"
log_both "Dataset: $DATASET"
log_both "Input: $INPUT_FILE"
log_both "Start time: $(date)"
log_both "Log file: $LOG_FILE"
log_both ""

SPARK_SCRIPT="$SCRIPT_DIR/spark_partitioning_optimized.py"

# Check files
if [ ! -f "$SPARK_SCRIPT" ]; then
    log_both "ERROR: Partitioning optimized Spark script not found: $SPARK_SCRIPT"
    exit 1
fi

FULL_INPUT_PATH="$DATA_DIR/$DATASET/$INPUT_FILE"
if [ ! -f "$FULL_INPUT_PATH" ]; then
    log_both "ERROR: Data file not found: $FULL_INPUT_PATH"
    exit 1
fi

FILE_SIZE=$(ls -lh "$FULL_INPUT_PATH" | awk '{print $5}')
FILE_SIZE_BYTES=$(ls -l "$FULL_INPUT_PATH" | awk '{print $5}')
log_both "Data file: $FILE_SIZE ($FILE_SIZE_BYTES bytes)"

mkdir -p "$(dirname "$OUTPUT_DIR")"

log_both ""
log_both "Running PARTITIONING OPTIMIZED Spark..."
log_both "=== SPARK OPTIMIZATION: PARTITIONING ==="
log_both "Optimized data partitioning for better parallelism"
log_both "Conservative memory allocation"
log_both "No caching to avoid memory constraints"
log_both "========================================="
log_both "Output: $OUTPUT_DIR"
log_both "Spark UI: http://localhost:4040"
log_both "----------------------------------------"

START_TIME=$(date +%s)

# Run partitioning optimized Spark with full logging
{
    spark-submit \
        --master local[*] \
        --conf "spark.executor.memory=1g" \
        --conf "spark.driver.memory=512m" \
        --conf "spark.default.parallelism=8" \
        --conf "spark.sql.shuffle.partitions=8" \
        --conf "spark.ui.enabled=true" \
        --conf "spark.ui.port=4040" \
        "$SPARK_SCRIPT" "$DATASET" "$FULL_INPUT_PATH" "$OUTPUT_DIR"
} 2>&1 | tee -a "$LOG_FILE"

EXIT_CODE=${PIPESTATUS[0]}
END_TIME=$(date +%s)
EXECUTION_TIME=$((END_TIME - START_TIME))

log_both "----------------------------------------"
log_both "Job exit status: $EXIT_CODE"

if [ $EXIT_CODE -eq 0 ]; then
    log_both "✓ PARTITIONING OPTIMIZED Spark completed successfully!"
    log_both "Execution time: $EXECUTION_TIME seconds"
    
    if [ -f "$OUTPUT_DIR/part-00000" ]; then
        log_both ""
        log_both "=== RESULTS ==="
        log_both "Sample distribution (first 10):"
        head -10 "$OUTPUT_DIR/part-00000" | tee -a "$LOG_FILE"
        
        TOTAL_ENTRIES=$(wc -l < "$OUTPUT_DIR/part-00000")
        OUTPUT_SIZE=$(ls -lh "$OUTPUT_DIR/part-00000" | awk '{print $5}')
        
        log_both ""
        log_both "Total distribution entries: $TOTAL_ENTRIES"
        log_both "Output file size: $OUTPUT_SIZE"
        
        log_both ""
        log_both "Top 5 highest in-degrees:"
        sort -k1 -nr "$OUTPUT_DIR/part-00000" | head -5 | tee -a "$LOG_FILE"
        
        log_both ""
        log_both "Full results saved to: $OUTPUT_DIR/part-00000"
    else
        log_both "WARNING: No output part files found!"
    fi
else
    log_both "✗ PARTITIONING OPTIMIZED Spark failed!"
    log_both "Exit code: $EXIT_CODE"
fi

log_both ""
log_both "=== PARTITIONING OPTIMIZATION SUMMARY ==="
log_both "Dataset: $DATASET"
log_both "Input file: $INPUT_FILE"
log_both "File size: $FILE_SIZE ($FILE_SIZE_BYTES bytes)"
log_both "Execution time: $EXECUTION_TIME seconds"
log_both "Job status: $([ $EXIT_CODE -eq 0 ] && echo 'SUCCESS' || echo 'FAILED')"
log_both "Mode: PARTITIONING OPTIMIZED Local"
log_both "End time: $(date)"

log_both ""
log_both "OPTIMIZATION DETAILS:"
log_both "  Applied: Data partitioning optimization"
log_both "  Memory: Conservative (1GB executor, 512MB driver)"
log_both "  Partitions: 16 input → 8 processing partitions"
log_both "  Parallelism: 8-way parallel processing"
log_both "  Strategy: No caching to avoid memory constraints"
log_both "  Status: $([ $EXIT_CODE -eq 0 ] && echo 'SUCCESS' || echo 'FAILED')"

log_both ""
log_both "COMPARISON TARGET:"
log_both "  Baseline Spark: 206 seconds"
log_both "  Caching Optimized: 264 seconds (memory constrained)"
log_both "  This run: $EXECUTION_TIME seconds"

log_both ""
log_both "LOG FILES SAVED:"
log_both "  Complete log: $LOG_FILE"
log_both "  Results: $OUTPUT_DIR/"
log_both "========================================"

echo ""
echo "IMPORTANT: Complete log saved to $LOG_FILE"
echo "This optimization avoids memory constraints while improving parallelism!"