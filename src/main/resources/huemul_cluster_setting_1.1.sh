#!/bin/bash
clear
echo "Creating HDFS Paths: START"
hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/data
hdfs dfs -mkdir /user/data/production
hdfs dfs -mkdir /user/data/production/temp
hdfs dfs -mkdir /user/data/production/raw
hdfs dfs -mkdir /user/data/production/master
hdfs dfs -mkdir /user/data/production/dim
hdfs dfs -mkdir /user/data/production/analytics
hdfs dfs -mkdir /user/data/production/reporting
hdfs dfs -mkdir /user/data/production/sandbox
hdfs dfs -mkdir /user/data/experimental
hdfs dfs -mkdir /user/data/experimental/temp
hdfs dfs -mkdir /user/data/experimental/raw
hdfs dfs -mkdir /user/data/experimental/master
hdfs dfs -mkdir /user/data/experimental/dim
hdfs dfs -mkdir /user/data/experimental/analytics
hdfs dfs -mkdir /user/data/experimental/reporting
hdfs dfs -mkdir /user/data/experimental/sandbox
echo "Creating HDFS Paths: FINISH"
echo "STARTING HIVE SETUP"
hive -e "CREATE DATABASE production_master"
hive -e "CREATE DATABASE experimental_master"
hive -e "CREATE DATABASE production_dim"
hive -e "CREATE DATABASE experimental_dim"
hive -e "CREATE DATABASE production_analytics"
hive -e "CREATE DATABASE experimental_analytics"
hive -e "CREATE DATABASE production_reporting"
hive -e "CREATE DATABASE experimental_reporting"
hive -e "CREATE DATABASE production_sandbox"
hive -e "CREATE DATABASE experimental_sandbox"
echo "STARTING HIVE SETUP"