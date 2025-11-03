#!/bin/bash

# Universal Plot Generator for In-Degree Distribution Analysis
# Works with any dataset: pokec, facebook, twitter, etc.

DATASET=$1
RESULTS_DIR=${2:-"~/bigdata-assignment/results"}
OUTPUT_DIR=${3:-"./plots"}

# Show usage if no dataset provided
if [ -z "$DATASET" ]; then
    echo "🎨 Universal In-Degree Distribution Plot Generator"
    echo "================================================="
    echo ""
    echo "Usage: $0 <dataset_name> [results_dir] [output_dir]"
    echo ""
    echo "Examples:"
    echo "  $0 pokec"
    echo "  $0 facebook"
    echo "  $0 twitter"
    echo "  $0 pokec ~/my-project/results ./my-plots"
    echo ""
    echo "Parameters:"
    echo "  dataset_name  : Name of the dataset (required)"
    echo "  results_dir   : Base results directory (default: ~/bigdata-assignment/results)"
    echo "  output_dir    : Output directory for plots (default: ./plots)"
    echo ""
    echo "Expected file structure:"
    echo "  results_dir/"
    echo "  ├── dataset_name/"
    echo "  │   ├── hadoop/output-*/part-r-00000"
    echo "  │   └── spark/final-output-*/part-00000"
    echo ""
    exit 1
fi

echo "🎨 Universal Plot Generator for Dataset: ${DATASET^^}"
echo "=================================================="

# Expand tilde in paths
RESULTS_DIR=$(eval echo $RESULTS_DIR)
OUTPUT_DIR=$(eval echo $OUTPUT_DIR)

echo "📍 Configuration:"
echo "   Dataset: $DATASET"
echo "   Results directory: $RESULTS_DIR"
echo "   Output directory: $OUTPUT_DIR"

# Check if results directory exists
if [ ! -d "$RESULTS_DIR" ]; then
    echo ""
    echo "❌ Results directory not found: $RESULTS_DIR"
    echo "💡 Please check the path or run your analyses first"
    exit 1
fi

# Check if dataset directory exists
DATASET_DIR="$RESULTS_DIR/$DATASET"
if [ ! -d "$DATASET_DIR" ]; then
    echo ""
    echo "❌ Dataset directory not found: $DATASET_DIR"
    echo "📁 Available datasets:"
    ls -1 "$RESULTS_DIR" 2>/dev/null | grep -v "^\\." | head -10
    echo ""
    echo "💡 Make sure you've run the analysis for dataset '$DATASET'"
    exit 1
fi

echo ""
echo "🔍 Checking for required Python packages..."

# Check and install required packages
python3 -c "import matplotlib" 2>/dev/null || {
    echo "📦 Installing matplotlib..."
    pip3 install matplotlib --user --quiet
}

python3 -c "import numpy" 2>/dev/null || {
    echo "📦 Installing numpy..."
    pip3 install numpy --user --quiet
}

echo "✅ Python packages ready!"

# Scan for result files
echo ""
echo "🔍 Scanning for result files in $DATASET_DIR..."

HADOOP_FILES=$(find "$DATASET_DIR" -path "*/hadoop/*" -name "part-r-00000" 2>/dev/null | wc -l)
SPARK_FILES=$(find "$DATASET_DIR" -path "*/spark/*" -name "part-00000" 2>/dev/null | wc -l)

echo "   📁 Hadoop result files found: $HADOOP_FILES"
echo "   📁 Spark result files found: $SPARK_FILES"

if [ "$HADOOP_FILES" -eq 0 ] && [ "$SPARK_FILES" -eq 0 ]; then
    echo ""
    echo "❌ No result files found for dataset '$DATASET'!"
    echo ""
    echo "📋 Expected file locations:"
    echo "   Hadoop: $DATASET_DIR/hadoop/output-*/part-r-00000"
    echo "   Spark:  $DATASET_DIR/spark/final-output-*/part-00000"
    echo ""
    echo "💡 Please run your analyses first:"
    echo "   ./hadoop/run_mapreduce.sh $DATASET input-file.txt"
    echo "   ./spark/run_spark.sh $DATASET input-file.txt"
    exit 1
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"
echo "📁 Created output directory: $OUTPUT_DIR"

echo ""
echo "🎨 Generating in-degree distribution plots for $DATASET..."
echo "=========================================================="

# Run the generic plot generator
python3 scripts/generate_plots.py "$DATASET" --results-dir "$RESULTS_DIR" --output-dir "$OUTPUT_DIR"

PLOT_EXIT_CODE=$?

echo ""
if [ $PLOT_EXIT_CODE -eq 0 ]; then
    echo "🎉 SUCCESS! Plots generated successfully!"
    echo ""
    echo "📊 Generated files in $OUTPUT_DIR:"
    ls -la "$OUTPUT_DIR"/${DATASET}_* 2>/dev/null || ls -la "$OUTPUT_DIR"
    echo ""
    echo "🎯 Main plot file:"
    MAIN_PLOT="$OUTPUT_DIR/${DATASET}_hadoop_vs_spark_comparison.png"
    if [ -f "$MAIN_PLOT" ]; then
        echo "   📁 Location: $MAIN_PLOT"
        echo "   📐 Resolution: 300 DPI (publication quality)"
        echo "   📋 Content: Comprehensive analysis with 6 plot types"
        echo ""
        echo "🖼️  To view the plot:"
        echo "   xdg-open \"$MAIN_PLOT\""
        echo "   # Or open the file in your image viewer"
    fi
    echo ""
    echo "💡 Use these plots in your assignment to demonstrate:"
    echo "   ✅ Power-law distribution analysis (log-log plot)"
    echo "   ✅ Network topology characteristics"
    echo "   ✅ Hadoop vs Spark result verification"
    echo "   ✅ Scale-free network properties"
    echo "   ✅ Computational correctness proof"
else
    echo "⚠️  Plot generation completed with issues."
    echo "📋 Check the output above for any error messages."
    echo ""
    echo "🛠️  Common troubleshooting:"
    echo "   • Ensure result files contain valid data"
    echo "   • Check file permissions"
    echo "   • Verify Python packages are installed"
fi

echo ""
echo "=========================================================="
echo "🏁 Plot generation complete for dataset: ${DATASET^^}"

# Show quick usage reminder
echo ""
echo "📝 Quick usage reminder:"
echo "   For other datasets: $0 facebook"
echo "   Custom paths: $0 twitter ~/custom/results ~/custom/plots"