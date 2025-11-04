1. Log into vm ubuntu
2. Open a terminal
3. Run below commands (one time only)

    mkdir -p bigdata-assignment/{data/{pokec,eu-email,berk},hadoop,spark,scripts,results}
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
7. Execute below commands to start or stop below services

   cd bigdata-assignment/scripts
   ./bigdata_services.sh start    # Start all services
   ./bigdata_services.sh stop     # Stop all services  
   ./bigdata_services.sh restart  # Restart all services
   ./bigdata_services.sh status   # Show current status

    YARN ResourceManager: http://localhost:8088
    HDFS NameNode: http://localhost:9870
    Hadoop Job History (if running): http://localhost:19888
    Spark Application UI: http://localhost:4040/
      

# From ~/bigdata-assignment/ directory:

# Test Hadoop MapReduce
# These commands copy the data files to HDFS and process no need of a manual copy
\n./hadoop/run_mapreduce.sh pokec soc-pokec-relationships.txt
\n./hadoop/run_mapreduce.sh eu-email Email-EuAll.txt
\n./hadoop/run_mapreduce.sh berk web-BerkStan.txt

# Manually Copy data to HDFS (Optional)
hdfs dfs -put /home/dinesh/bigdata-assignment/data/pokec/soc-pokec-relationships.txt /input/pokec/
hdfs dfs -put /home/dinesh/bigdata-assignment/data/eu-email/Email-EuAll.txt /input/eu-email/
hdfs dfs -put /home/dinesh/bigdata-assignment/data/berk/web-BerkStan.txt /input/berk/


# Create directories for all your datasets
hdfs dfs -mkdir -p /input/pokec
hdfs dfs -mkdir -p /input/eu-email  
hdfs dfs -mkdir -p /input/berk

# Verify they're created
hdfs dfs -ls /input

# Test Spark  
./spark/run_spark.sh pokec soc-pokec-relationships.txt
./spark/run_spark.sh eu-email Email-EuAll.txt
./spark/run_spark.sh berk web-BerkStan.txt