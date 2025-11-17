# Task 2: Real-Time IoT Sensor Data Analytics using Apache Kafka

## Project Environment Set Up - (Inside VM Ubuntu)
### After setting up the Kafka environment
### First time set up:
</br> 1. Open a command promt
</br> 2. Run manage_kafka_ip.sh - This makes Kafka set up IP address static
</br> 3. Run configure_kafka_persistence.sh (This will create Kafka and Zookeper persistant storage)
</br> 4. Run kafka-topic-creation.sh (This will create source and destination Kafka topics)
</br> 5. (Optional) Run kafka-clean-up-commands.sh (This will delete existing Kafka topics and Consumer Group)
</br> 6. Run ./kafka-control.sh start (This will start Kafka and Zookeepr)
</br> 7. (Optional) Run ./kafka-control.sh stop (This will gracefully shutdown Kafka and Zookeepr)

## Project Environment Set Up - (Inside Host)
</br> 1. Open a command prompt
</br> 2. Run command -> cd t2
### First time set up:
</br> 3. Run command -> python -m venv t2-env (Setting up python environment)
</br> 4. Run command -> t2-env\Scripts\activate (Activate python enviroment)
</br> 5. Run command -> pip install -r requirements.txt (Install packages)
### Otherwise:
</br> 6. Run command -> t2-env\Scripts\activate (Activate python enviroment)
</br> 7. Run command -> docker-compose up -d (This will run Kafdrop, Grafana and Postgres as docker containers with volum mount)
</br> 8. Download traffic dataset and put in data folder
</br> 9. Open Postgresql and create a database called traffic-sensor and then open a Query window and run schema.sql script (This will create db tables, indexes, grafana user and its permissions)
</br> 10. (Optional) Run db-clean-up-commands.sql (This will clean up data in tables)

## How to run the application
### In host:
</br> 1. Open 2 command prompts
</br> 2. Run command -> cd t2
</br> 3. Run command -> python -m venv t2-env (Setting up python environment)
</br> 4. Run command -> t2-env\Scripts\activate (Activate python enviroment)
</br> 5. In prompt 1 Run command -> python producer.py
</br> 6. In prompt 2 Run command -> python consumer.py