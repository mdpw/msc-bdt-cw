1. Log into vm ubuntu
2. Open a terminal
3. Run below commands (one time only)
    </br>mkdir -p bigdata-assignment/{data/{pokec,eu-email,berk},hadoop,spark,scripts,results}
    </br>cd bigdata-assignment

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
   </br>./bigdata_services.sh start    # Start all services
   </br>./bigdata_services.sh stop     # Stop all services  
   </br>./bigdata_services.sh restart  # Restart all services
   </br>./bigdata_services.sh status   # Show current status

    </br>YARN ResourceManager: http://localhost:8088
    </br>HDFS NameNode: http://localhost:9870
    </br>Hadoop Job History (if running): http://localhost:19888
    </br>Spark Application UI: http://localhost:4040/
      

# From ~/bigdata-assignment/ directory:

# Test Hadoop MapReduce
# These commands copy the data files to HDFS and process no need of a manual copy
</br>./hadoop/run_mapreduce.sh pokec soc-pokec-relationships.txt
</br>./hadoop/run_mapreduce.sh eu-email Email-EuAll.txt
</br>./hadoop/run_mapreduce.sh berk web-BerkStan.txt

# Manually Copy data to HDFS (Optional)
</br>hdfs dfs -put /home/dinesh/bigdata-assignment/data/pokec/soc-pokec-relationships.txt /input/pokec/
</br>hdfs dfs -put /home/dinesh/bigdata-assignment/data/eu-email/Email-EuAll.txt /input/eu-email/
</br>hdfs dfs -put /home/dinesh/bigdata-assignment/data/berk/web-BerkStan.txt /input/berk/


# Create directories for all your datasets
</br>hdfs dfs -mkdir -p /input/pokec
</br>hdfs dfs -mkdir -p /input/eu-email  
</br>hdfs dfs -mkdir -p /input/berk

# Verify they're created
</br>hdfs dfs -ls /input

# Test Spark  
</br>./spark/run_spark.sh pokec soc-pokec-relationships.txt
</br>./spark/run_spark.sh eu-email Email-EuAll.txt
</br>./spark/run_spark.sh berk web-BerkStan.txt
