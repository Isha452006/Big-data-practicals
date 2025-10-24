# Practical 4: Apache Pig – Data Flow Scripting

**Objective:** Process and transform large datasets using Pig Latin scripts.  
**Tools Used:** Apache Pig, Hadoop, HDFS

## Commands
```bash
hdfs dfs -mkdir -p /user/hadoop
hdfs dfs -put students.csv /user/hadoop/
pig practical4.pig
hdfs dfs -cat /user/hadoop/output_high_marks/part-r-00000
hdfs dfs -cat /user/hadoop/output_counts/part-r-00000
