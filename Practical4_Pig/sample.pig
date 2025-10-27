-- Sample Pig Latin Script for Practical 4

-- Load CSV data from HDFS
students = LOAD 'students.csv' USING PigStorage(',') AS (id:int, name:chararray, score:int);

-- Filter students who passed (score >= 40)
passed = FILTER students BY score >= 40;

-- Group students by score
grouped = GROUP passed BY score;

-- Display grouped results
DUMP grouped;

-- Store filtered results back to HDFS
STORE passed INTO 'output/passed_students' USING PigStorage(',');
