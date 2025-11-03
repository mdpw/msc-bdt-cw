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

echo "========================================"
echo "SPARK IN-DEGREE ANALYSIS (LOCAL MODE): $DATASET"
echo "========================================"
echo "Input file: $INPUT_FILE"
echo "Start time: $(date)"
echo ""

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Set paths relative to project structure
DATA_DIR="$PROJECT_ROOT/data"
OUTPUT_DIR="$PROJECT_ROOT/results/$DATASET/spark/final-output-$(date +%Y%m%d_%H%M%S)"

echo "Project root: $PROJECT_ROOT"
echo "Data directory: $DATA_DIR"
echo "Output directory: $OUTPUT_DIR"
echo ""

# Force local mode
export USE_YARN=false
echo "Execution mode: LOCAL (forced)"

# Use the final comprehensive Spark script
SPARK_SCRIPT="$SCRIPT_DIR/spark_indegree_analysis_final.py"

if [ ! -f "$SPARK_SCRIPT" ]; then
    echo "ERROR: Final Spark script not found at $SPARK_SCRIPT"
    echo "Looking for uploaded script..."
    SPARK_SCRIPT="$SCRIPT_DIR/spark_indegree_analysis.py"
    if [ ! -f "$SPARK_SCRIPT" ]; then
        echo "ERROR: No Spark script found!"
        exit 1
    fi
fi

echo "Using Spark script: $SPARK_SCRIPT"

# Check if data file exists
FULL_INPUT_PATH="$DATA_DIR/$DATASET/$INPUT_FILE"
if [ ! -f "$FULL_INPUT_PATH" ]; then
    echo "ERROR: Data file $FULL_INPUT_PATH not found!"
    echo "Available files in $DATA_DIR/$DATASET:"
    ls -la "$DATA_DIR/$DATASET" 2>/dev/null || echo "Directory does not exist"
    exit 1
fi

# Install required Python packages if not available
echo "🔧 Checking Python dependencies..."
python3 -c "import psutil" 2>/dev/null || {
    echo "📦 Installing psutil for system metrics..."
    pip3 install psutil --user --quiet || {
        echo "⚠️ Could not install psutil. System metrics will be limited."
    }
}

# Setup event logging directory for Spark UI
EVENT_LOG_DIR="/tmp/spark-events"
mkdir -p "$EVENT_LOG_DIR"
echo "Event log directory: $EVENT_LOG_DIR"

# Create output directory
mkdir -p "$(dirname "$OUTPUT_DIR")"

# Get file size for reporting
FILE_SIZE=$(ls -lh "$FULL_INPUT_PATH" | awk '{print $5}')
echo ""
echo "Dataset: $DATASET"
echo "Dataset size: $FILE_SIZE"
echo "Input path: $FULL_INPUT_PATH"
echo "Output path: $OUTPUT_DIR"
echo ""

echo "Starting Spark application in LOCAL mode..."
echo "📊 Monitor job progress:"
echo "  📊 Spark UI: http://localhost:4040"
echo "  📈 Spark History: http://localhost:18080"
echo "----------------------------------------"

START_TIME=$(date +%s)

# Run the Spark job with comprehensive configurations
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

# Capture exit status
JOB_STATUS=$?
END_TIME=$(date +%s)
EXECUTION_TIME=$((END_TIME - START_TIME))

echo "----------------------------------------"
echo "Job exit status: $JOB_STATUS"

if [ $JOB_STATUS -eq 0 ]; then
    echo "🎉 Spark job completed successfully!"
    
    # Check if output exists
    echo ""
    echo "🔍 Checking output..."
    ls -la "$OUTPUT_DIR"/ 2>/dev/null
    
    if [ -f "$OUTPUT_DIR/part-00000" ]; then
        echo ""
        echo "=== RESULTS VERIFICATION ==="
        echo "Sample distribution (first 5):"
        head -5 "$OUTPUT_DIR/part-00000"
        
        # Count total entries
        TOTAL_ENTRIES=$(wc -l < "$OUTPUT_DIR/part-00000")
        echo ""
        echo "📈 Total distribution entries: $TOTAL_ENTRIES"
        
        # Show output size
        OUTPUT_SIZE=$(ls -lh "$OUTPUT_DIR/part-00000" | awk '{print $5}')
        echo "💾 Output file size: $OUTPUT_SIZE"
        
        # Show top 3 highest degrees
        echo ""
        echo "🏆 Top 3 highest in-degrees:"
        sort -k1 -nr "$OUTPUT_DIR/part-00000" | head -3
        
    else
        echo "⚠️ WARNING: No output part files found!"
        echo "Output directory contents:"
        ls -la "$OUTPUT_DIR"/ || echo "Output directory doesn't exist"
    fi
    
    # Show comprehensive summary if exists
    if [ -f "${OUTPUT_DIR}_comprehensive_summary.txt" ]; then
        echo ""
        echo "=== 📊 COMPREHENSIVE METRICS SUMMARY ==="
        echo "📁 Detailed metrics: ${OUTPUT_DIR}_comprehensive_summary.txt"
        echo ""
        echo "Key Performance Indicators:"
        grep "TOTAL EXECUTION TIME" "${OUTPUT_DIR}_comprehensive_summary.txt" || echo "Execution time data available in summary file"
    fi
    
    echo ""
    echo "🎯 SPARK UI ANALYSIS LINKS:"
    echo "  📊 Live Spark UI: http://localhost:4040"
    echo "     • Jobs tab: Execution time breakdown"
    echo "     • Stages tab: Task-level performance"
    echo "     • Executors tab: Memory and CPU usage"
    echo "     • Storage tab: RDD caching info"
    echo "  📈 Spark History: http://localhost:18080"
    echo "  📋 Event logs: $EVENT_LOG_DIR"
    echo ""
    echo "💡 METRICS FOR HADOOP COMPARISON:"
    echo "  ⏱️ Execution Times: Available in console output above"
    echo "  💾 Memory Usage: Available in Spark UI Executors tab"
    echo "  🖥️ CPU Usage: Available in Spark UI and console output"
    echo "  📊 I/O Metrics: Available in Spark UI Stages tab"
    echo "  📈 Task Performance: Available in Spark UI Jobs tab"
    
else
    echo "❌ Spark job failed with exit code: $JOB_STATUS"
    echo ""
    echo "🛠️ Troubleshooting suggestions:"
    echo "- Check if Python/PySpark is properly installed"
    echo "- Verify input file format and permissions"
    echo "- Try reducing memory: --conf spark.executor.memory=1g"
    echo "- Check the error messages above for specific issues"
fi

echo ""
echo "=== FINAL EXECUTION SUMMARY ==="
echo "Mode: LOCAL"
echo "Dataset: $DATASET"
echo "Input file: $INPUT_FILE"
echo "File size: $FILE_SIZE"
echo "Shell execution time: $EXECUTION_TIME seconds"
echo "Job status: $([ $JOB_STATUS -eq 0 ] && echo '✅ SUCCESS' || echo '❌ FAILED')"
echo "End time: $(date)"

echo ""
echo "📁 Output location: $OUTPUT_DIR"
echo "🔍 To view full results: cat $OUTPUT_DIR/part-00000"

if [ $JOB_STATUS -eq 0 ]; then
    echo ""
    echo "🎯 READY FOR HADOOP COMPARISON:"
    echo "✅ Comprehensive metrics captured in both console and Spark UI"
    echo "📊 Screenshots recommended from Spark UI at http://localhost:4040"
    echo "📄 Detailed summary available in: ${OUTPUT_DIR}_comprehensive_summary.txt"
    echo ""
    echo "📊 Spark UI: http://localhost:4040"
    echo "📈 History: http://localhost:18080"
fi

echo ""
echo "========================================"