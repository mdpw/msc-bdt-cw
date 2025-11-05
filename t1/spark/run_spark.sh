#!/bin/bash

# Spark runner with comprehensive metrics - LOCAL mode only
# Usage: ./run_spark.sh <dataset_name> <input_file>

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
RESULTS_DIR="$PROJECT_ROOT/results/$DATASET/spark"

# Create results directory
mkdir -p "$RESULTS_DIR"

# Create log file with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$RESULTS_DIR/spark_baseline_$TIMESTAMP.log"
OUTPUT_DIR="$RESULTS_DIR/final-output-$TIMESTAMP"

# Function to log both to console and file
log_both() {
    echo "$1" | tee -a "$LOG_FILE"
}

log_both "========================================"
log_both "SPARK IN-DEGREE ANALYSIS (LOCAL MODE): $DATASET"
log_both "========================================"
log_both "Input file: $INPUT_FILE"
log_both "Start time: $(date)"
log_both "Log file: $LOG_FILE"
log_both ""

log_both "Project root: $PROJECT_ROOT"
log_both "Data directory: $DATA_DIR"
log_both "Output directory: $OUTPUT_DIR"
log_both ""

# Force local mode
export USE_YARN=false
log_both "Execution mode: LOCAL (forced)"

# Use the final comprehensive Spark script
SPARK_SCRIPT="$SCRIPT_DIR/spark_indegree_analysis_final.py"

if [ ! -f "$SPARK_SCRIPT" ]; then
    log_both "ERROR: Final Spark script not found at $SPARK_SCRIPT"
    log_both "Looking for uploaded script..."
    SPARK_SCRIPT="$SCRIPT_DIR/spark_indegree_analysis.py"
    if [ ! -f "$SPARK_SCRIPT" ]; then
        log_both "ERROR: No Spark script found!"
        exit 1
    fi
fi

log_both "Using Spark script: $SPARK_SCRIPT"

# Check if data file exists
FULL_INPUT_PATH="$DATA_DIR/$DATASET/$INPUT_FILE"
if [ ! -f "$FULL_INPUT_PATH" ]; then
    log_both "ERROR: Data file $FULL_INPUT_PATH not found!"
    log_both "Available files in $DATA_DIR/$DATASET:"
    ls -la "$DATA_DIR/$DATASET" 2>/dev/null | tee -a "$LOG_FILE" || log_both "Directory does not exist"
    exit 1
fi

# Install required Python packages if not available
log_both "Checking Python dependencies..."
python3 -c "import psutil" 2>/dev/null || {
    log_both "Installing psutil for system metrics..."
    pip3 install psutil --user --quiet || {
        log_both "Could not install psutil. System metrics will be limited."
    }
}

# Setup event logging directory for Spark UI
EVENT_LOG_DIR="/tmp/spark-events"
mkdir -p "$EVENT_LOG_DIR"
log_both "Event log directory: $EVENT_LOG_DIR"

# Create output directory
mkdir -p "$(dirname "$OUTPUT_DIR")"

# Get file size for reporting
FILE_SIZE=$(ls -lh "$FULL_INPUT_PATH" | awk '{print $5}')
FILE_SIZE_BYTES=$(ls -l "$FULL_INPUT_PATH" | awk '{print $5}')
log_both ""
log_both "Dataset: $DATASET"
log_both "Dataset size: $FILE_SIZE ($FILE_SIZE_BYTES bytes)"
log_both "Input path: $FULL_INPUT_PATH"
log_both "Output path: $OUTPUT_DIR"
log_both ""

log_both "Starting Spark application in LOCAL mode..."
log_both "Monitor job progress:"
log_both "  Spark UI: http://localhost:4040"
log_both "  Spark History: http://localhost:18080"
log_both "----------------------------------------"

START_TIME=$(date +%s)

# Run the Spark job with comprehensive configurations and full logging
{
    spark-submit \
        --master local[*] \
        --conf "spark.eventLog.enabled=true" \
        --conf "spark.eventLog.dir=$EVENT_LOG_DIR" \
        --conf "spark.ui.enabled=true" \
        --conf "spark.ui.port=4040" \
        --conf "spark.executor.memory=2g" \
        --conf "spark.driver.memory=1g" \
        --conf "spark.hadoop.fs.defaultFS=file:///" \
        --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
        "$SPARK_SCRIPT" "$DATASET" "$FULL_INPUT_PATH" "$OUTPUT_DIR"
} 2>&1 | tee -a "$LOG_FILE"

# Capture exit status
JOB_STATUS=${PIPESTATUS[0]}
END_TIME=$(date +%s)
EXECUTION_TIME=$((END_TIME - START_TIME))

log_both "----------------------------------------"
log_both "Job exit status: $JOB_STATUS"

if [ $JOB_STATUS -eq 0 ]; then
    log_both "Spark job completed successfully!"
    
    # Check if output exists
    log_both ""
    log_both "Checking output..."
    ls -la "$OUTPUT_DIR"/ 2>/dev/null | tee -a "$LOG_FILE"
    
    if [ -f "$OUTPUT_DIR/part-00000" ]; then
        log_both ""
        log_both "=== RESULTS VERIFICATION ==="
        log_both "Sample distribution (first 5):"
        head -5 "$OUTPUT_DIR/part-00000" | tee -a "$LOG_FILE"
        
        # Count total entries
        TOTAL_ENTRIES=$(wc -l < "$OUTPUT_DIR/part-00000")
        log_both ""
        log_both "Total distribution entries: $TOTAL_ENTRIES"
        
        # Show output size
        OUTPUT_SIZE=$(ls -lh "$OUTPUT_DIR/part-00000" | awk '{print $5}')
        log_both "Output file size: $OUTPUT_SIZE"
        
        # Show top 3 highest degrees
        log_both ""
        log_both "Top 3 highest in-degrees:"
        sort -k1 -nr "$OUTPUT_DIR/part-00000" | head -3 | tee -a "$LOG_FILE"
        
    else
        log_both "WARNING: No output part files found!"
        log_both "Output directory contents:"
        ls -la "$OUTPUT_DIR"/ | tee -a "$LOG_FILE" || log_both "Output directory doesn't exist"
    fi
    
    # Show comprehensive summary if exists
    if [ -f "${OUTPUT_DIR}_comprehensive_summary.txt" ]; then
        log_both ""
        log_both "=== COMPREHENSIVE METRICS SUMMARY ==="
        log_both "Detailed metrics: ${OUTPUT_DIR}_comprehensive_summary.txt"
        log_both ""
        log_both "Key Performance Indicators:"
        grep "TOTAL EXECUTION TIME" "${OUTPUT_DIR}_comprehensive_summary.txt" | tee -a "$LOG_FILE" || log_both "Execution time data available in summary file"
    fi
    
    log_both ""
    log_both "SPARK UI ANALYSIS LINKS:"
    log_both "  Live Spark UI: http://localhost:4040"
    log_both "     Jobs tab: Execution time breakdown"
    log_both "     Stages tab: Task-level performance"
    log_both "     Executors tab: Memory and CPU usage"
    log_both "     Storage tab: RDD caching info"
    log_both "  Spark History: http://localhost:18080"
    log_both "  Event logs: $EVENT_LOG_DIR"
    log_both ""
    log_both "METRICS FOR HADOOP COMPARISON:"
    log_both "  Execution Times: Available in console output above"
    log_both "  Memory Usage: Available in Spark UI Executors tab"
    log_both "  CPU Usage: Available in Spark UI and console output"
    log_both "  I/O Metrics: Available in Spark UI Stages tab"
    log_both "  Task Performance: Available in Spark UI Jobs tab"
    
else
    log_both "Spark job failed with exit code: $JOB_STATUS"
    log_both ""
    log_both "Troubleshooting suggestions:"
    log_both "- Check if Python/PySpark is properly installed"
    log_both "- Verify input file format and permissions"
    log_both "- Try reducing memory: --conf spark.executor.memory=1g"
    log_both "- Check the error messages above for specific issues"
fi

log_both ""
log_both "=== FINAL EXECUTION SUMMARY ==="
log_both "Mode: LOCAL"
log_both "Dataset: $DATASET"
log_both "Input file: $INPUT_FILE"
log_both "File size: $FILE_SIZE ($FILE_SIZE_BYTES bytes)"
log_both "Shell execution time: $EXECUTION_TIME seconds"
log_both "Job status: $([ $JOB_STATUS -eq 0 ] && echo 'SUCCESS' || echo 'FAILED')"
log_both "End time: $(date)"

log_both ""
log_both "Output location: $OUTPUT_DIR"
log_both "To view full results: cat $OUTPUT_DIR/part-00000"

if [ $JOB_STATUS -eq 0 ]; then
    log_both ""
    log_both "READY FOR HADOOP COMPARISON:"
    log_both "Comprehensive metrics captured in both console and Spark UI"
    log_both "Screenshots recommended from Spark UI at http://localhost:4040"
    log_both "Detailed summary available in: ${OUTPUT_DIR}_comprehensive_summary.txt"
    log_both ""
    log_both "Spark UI: http://localhost:4040"
    log_both "History: http://localhost:18080"
fi

log_both ""
log_both "LOG FILES SAVED:"
log_both "  Complete log: $LOG_FILE"
log_both "  Results: $OUTPUT_DIR/"
log_both "========================================"

echo ""
echo "IMPORTANT: Complete log saved to $LOG_FILE"
echo "Even if terminal gets stuck, check this file for full results!"