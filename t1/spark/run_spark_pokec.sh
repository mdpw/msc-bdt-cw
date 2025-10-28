#!/bin/bash

# Simple Spark runner for Pokec in-degree analysis
# Usage: ./run_spark_pokec.sh

INPUT_FILE="/data/pokec/soc-pokec-relationships.txt"
OUTPUT_DIR="/output/spark-pokec-$(date +%Y%m%d_%H%M%S)"

echo "========================================"
echo "SPARK POKEC IN-DEGREE ANALYSIS"
echo "========================================"
echo "Input file: $INPUT_FILE"
echo "Output directory: $OUTPUT_DIR"
echo "Start time: $(date)"
echo ""

# Check if input file exists
if [ ! -f "$INPUT_FILE" ]; then
    echo "ERROR: Input file not found at $INPUT_FILE"
    echo "Please ensure the Pokec dataset is available"
    exit 1
fi

# Create output directory
mkdir -p $(dirname "$OUTPUT_DIR")

# Remove existing output if it exists
if [ -d "$OUTPUT_DIR" ]; then
    echo "Removing existing output directory..."
    rm -rf "$OUTPUT_DIR"
fi

# Run Spark analysis
echo "Starting Spark analysis..."
python3 spark_pokec_indegree.py "$INPUT_FILE" "$OUTPUT_DIR"

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ Analysis completed successfully!"
    echo ""
    echo "Sample results:"
    head -5 "$OUTPUT_DIR/part-00000" 2>/dev/null || echo "No results file found"
    echo ""
    echo "Complete results in: $OUTPUT_DIR"
else
    echo "✗ Analysis failed!"
    exit 1
fi

echo "========================================"