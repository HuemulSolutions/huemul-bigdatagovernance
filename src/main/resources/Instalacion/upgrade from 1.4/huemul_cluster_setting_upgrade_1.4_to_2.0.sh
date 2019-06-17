#!/bin/bash
clear
echo "Creating HDFS Paths: START"
hdfs dfs -mkdir /user/data/production/mdm_oldvalue
hdfs dfs -mkdir /user/data/experimental/mdm_oldvalue
echo "Creating HDFS Paths: FINISH"
echo "STARTING HIVE SETUP"
hive -e "CREATE DATABASE production_mdm_oldvalue"
hive -e "CREATE DATABASE experimental_mdm_oldvalue"
echo "STARTING HIVE SETUP"