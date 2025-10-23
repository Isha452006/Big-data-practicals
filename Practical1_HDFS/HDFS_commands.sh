#!/bin/bash
# Practical 1: HDFS Commands in Cloudera

hdfs dfs -mkdir /mydata
hdfs dfs -put sample.txt /mydata
hdfs dfs -ls /mydata
hdfs dfs -cat /mydata/sample.txt
hdfs fsck /mydata/sample.txt -files -blocks -locations
hdfs dfsadmin -report
