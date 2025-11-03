1. Log into vm ubuntu
2. Open a terminal
3. Run below commands (one time only)

    mkdir -p bigdata-assignment/{data/{pokec,facebook,twitter},hadoop,spark,scripts,results}
    cd bigdata-assignment

4. Place datasets in respective folders
5. Make sure below folder structure and files exists

~/bigdata-assignment/
├── data/
│   ├── pokec/
│   │   └── soc-pokec-relationships.txt
│   ├── eu-email/
│   │   └── Email-EuAll.txt
│   └── berk/
│       └── web-BerkStan.txt
│
├── hadoop/
│   ├── InDegreeMapper.java
│   ├── InDegreeReducer.java
│   ├── DistributionMapper.java
│   ├── DistributionReducer.java
│   ├── InDegreeDistributionDriver.java
│   └── run_mapreduce.sh
│
├── spark/
│   ├── spark_indegree_analysis.py
│   └── run_spark.sh
│
└── results/
    ├── pokec/
    │   ├── hadoop/
    │   └── spark/
    ├── eu-email/
    │   ├── hadoop/
    │   └── spark/
    └── berk/
        ├── hadoop/
        └── spark/

6. Open a terminal in ubuntu
7. Execute below commands to start below services
    YARN ResourceManager: http://localhost:8088
    HDFS NameNode: http://localhost:9870
    
    
    stop-yarn.sh
    stop-dfs.sh
    start-dfs.sh
    start-yarn.sh
    jps

8. Start Job History Server (Optional but Recommended)
    mr-jobhistory-daemon.sh start historyserver

    Hadoop Job History (if running): http://localhost:19888


cd ~/bigdata-assignment

# From ~/bigdata-assignment/ directory:

# Test Hadoop MapReduce
./hadoop/run_mapreduce.sh pokec soc-pokec-relationships.txt


Create All Dataset Directories for spark
# Create directories for all your datasets
hdfs dfs -mkdir -p /input/pokec
hdfs dfs -mkdir -p /input/eu-email  
hdfs dfs -mkdir -p /input/berk

# Verify they're created
hdfs dfs -ls /input

# Copy data to HDFS
hdfs dfs -put /home/dinesh/bigdata-assignment/data/pokec/soc-pokec-relationships.txt /input/pokec/
hdfs dfs -put /home/dinesh/bigdata-assignment/data/eu-email/Email-EuAll.txt /input/eu-email/
hdfs dfs -put /home/dinesh/bigdata-assignment/data/berk/web-BerkStan.txt /input/berk/

# Test Spark  
./spark/run_spark.sh pokec soc-pokec-relationships.txt


## 📊 **What to Monitor During Experiments**

### **🎯 YARN ResourceManager UI (http://localhost:8088)**

**📸 Screenshot These Pages:**

1. **Applications Tab**
   - Shows running/completed applications
   - Look for your "InDegreeAnalysis" jobs
   - **Monitor**: Application state, progress, memory allocation

2. **Individual Application Details** (click on app)
   - **Monitor**: Memory usage, CPU utilization, containers
   - **Screenshot**: Resource allocation graphs
   - **Look for**: Peak memory usage, execution timeline

3. **Cluster Info**
   - **Monitor**: Total cluster resources, utilization %
   - **Screenshot**: Overall cluster health

### **🎯 Job History Server UI (http://localhost:19888)**

**📸 Screenshot These Pages:**

1. **Completed Jobs List**
   - **Monitor**: Job completion times, success/failure rates
   - **Screenshot**: List showing your MapReduce jobs

2. **Individual Job Details** (click on job)
   - **Monitor**: Maps/Reduces completed, data processed
   - **Screenshot**: Job execution timeline, task details

3. **Job Counters**
   - **Monitor**: Bytes read/written, records processed
   - **Screenshot**: Detailed performance counters

### **🎯 HDFS NameNode UI (http://localhost:9870)**

**📸 Screenshot These Pages:**

1. **Overview**
   - **Monitor**: HDFS capacity, utilization
   - **Screenshot**: Storage usage statistics

## 📋 **Performance Metrics Checklist**

### **⏱️ Execution Time Metrics:**
- [ ] Job start/finish timestamps
- [ ] Total execution duration
- [ ] Map phase vs Reduce phase timing (Hadoop)
- [ ] Stage execution times (Spark)

### **💾 Memory Usage Metrics:**
- [ ] Allocated memory per application
- [ ] Peak memory utilization
- [ ] Memory efficiency (% actually used)
- [ ] Container memory allocation

### **🖥️ CPU Utilization:**
- [ ] VCore allocation
- [ ] CPU utilization percentage
- [ ] Task parallelization efficiency

### **📊 Data Processing Metrics:**
- [ ] Input data size (bytes)
- [ ] Output data size (bytes)
- [ ] Records processed per second
- [ ] Data locality (local vs remote reads)

### **🔄 System Metrics:**
- [ ] Number of containers used
- [ ] Task success/failure rates
- [ ] Disk I/O operations
- [ ] Network data transfer

## 🎨 **Screenshot Strategy**

### **📸 During Each Experiment:**

1. **Before Starting:**
   - Clean YARN applications page
   - HDFS overview showing available space

2. **During Execution:**
   - YARN showing job running (progress bars)
   - Resource allocation in real-time
   - Container allocation graphs

3. **After Completion:**
   - Completed application details
   - Job history server metrics
   - Final resource utilization

### **📁 Organize Screenshots:**
```
screenshots/
├── yarn/
│   ├── pokec_hadoop_running.png
│   ├── pokec_spark_running.png
│   ├── applications_overview.png
│   └── resource_allocation.png
├── job_history/
│   ├── hadoop_job_details.png
│   ├── job_counters.png
│   └── task_timeline.png
└── comparison/
    ├── execution_times.png
    └── resource_usage.png



./bigdata_services.sh start    # Start all services
./bigdata_services.sh stop     # Stop all services  
./bigdata_services.sh restart  # Restart all services
./bigdata_services.sh status   # Show current status