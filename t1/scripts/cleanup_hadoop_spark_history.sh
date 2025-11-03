#!/bin/bash

echo "🧹 Complete Hadoop/Spark cleanup including Job History..."

# Stop all services
echo "Stopping all services..."
stop-yarn.sh 2>/dev/null
stop-dfs.sh 2>/dev/null
stop-history-server.sh 2>/dev/null
mapred --daemon stop historyserver 2>/dev/null

echo "Cleaning YARN logs and history..."
rm -rf $HADOOP_HOME/logs/userlogs/* 2>/dev/null
rm -rf /tmp/hadoop-$USER/yarn/local/userlogs/* 2>/dev/null
rm -rf /tmp/hadoop-yarn-timeline/* 2>/dev/null
rm -rf $HADOOP_HOME/logs/*.log* 2>/dev/null
rm -rf $HADOOP_HOME/logs/*.out* 2>/dev/null

echo "Cleaning MapReduce Job History..."
rm -rf /tmp/hadoop-yarn/staging/history/done/* 2>/dev/null
rm -rf /tmp/hadoop-yarn/staging/history/done_intermediate/* 2>/dev/null
rm -rf /tmp/hadoop-mapred/history/* 2>/dev/null

echo "Cleaning Spark logs and history..."
rm -rf /tmp/spark-events/* 2>/dev/null
rm -rf $SPARK_HOME/logs/* 2>/dev/null
rm -rf $SPARK_HOME/work/* 2>/dev/null
rm -rf /tmp/spark-* 2>/dev/null

echo "Cleaning HDFS job history..."
start-dfs.sh
sleep 5
hdfs dfs -rm -r /tmp/hadoop-yarn/staging/history/* 2>/dev/null
hdfs dfs -rm -r /user/history/* 2>/dev/null
hdfs dfs -rm -r /mr-history/* 2>/dev/null

echo "Restarting services..."
start-yarn.sh
mapred --daemon start historyserver
start-history-server.sh

echo "✅ Complete cleanup finished!"
echo ""
echo "🎯 Check these URLs - they should be clean:"
echo "📊 YARN UI: http://localhost:8088"
echo "📋 Job History: http://dinesh-virtualbox:19888/jobhistory"
echo "⚡ Spark History: http://localhost:18080"

# Wait a moment then check
sleep 10
echo ""
echo "🔍 Verification:"
echo "YARN Applications: $(curl -s http://localhost:8088/ws/v1/cluster/apps | grep -o '"totalApplications":[0-9]*' | cut -d: -f2)"