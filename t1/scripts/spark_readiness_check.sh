#!/bin/bash

# Simple Spark status checker for native Ubuntu installation

echo "SPARK STATUS CHECK"
echo "=================="

# Check if Spark is installed
if command -v spark-submit &> /dev/null; then
    echo "✓ spark-submit found"
    spark-submit --version 2>&1 | head -1
else
    echo "✗ spark-submit not found"
fi

if command -v pyspark &> /dev/null; then
    echo "✓ pyspark command found"
else
    echo "✗ pyspark command not found"
fi

# Check PySpark Python library
if python3 -c "import pyspark; print('✓ PySpark library version:', pyspark.__version__)" 2>/dev/null; then
    echo "✓ PySpark Python library is available"
else
    echo "✗ PySpark Python library not installed"
    echo "  Install with: pip3 install pyspark"
fi

# Check for running Spark processes
spark_procs=$(ps aux | grep -E "spark.*[Mm]aster|spark.*[Ww]orker" | grep -v grep | wc -l)
if [ $spark_procs -gt 0 ]; then
    echo "✓ Found $spark_procs Spark processes running"
    ps aux | grep -E "spark.*[Mm]aster|spark.*[Ww]orker" | grep -v grep
else
    echo "✗ No Spark cluster processes running"
    echo "  For standalone cluster, start with:"
    echo "    \$SPARK_HOME/sbin/start-master.sh"
    echo "    \$SPARK_HOME/sbin/start-workers.sh"
fi

# Check Spark web UI
if curl -s --connect-timeout 3 http://localhost:8080 >/dev/null 2>&1; then
    echo "✓ Spark Master Web UI accessible at http://localhost:8080"
else
    echo "✗ Spark Master Web UI not accessible"
fi

# Check HDFS if available
if command -v hdfs &> /dev/null; then
    echo "✓ HDFS command available"
    if hdfs dfsadmin -report &>/dev/null; then
        echo "✓ HDFS is running"
    else
        echo "✗ HDFS is not accessible"
    fi
else
    echo "✗ HDFS not found (optional for local file analysis)"
fi

echo ""
echo "RECOMMENDATION:"
echo "For this assignment, you can run Spark in local mode without a cluster."
echo "The script will use 'local[*]' master URL to utilize all CPU cores."