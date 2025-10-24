students = LOAD '/user/hadoop/students.csv' 
    USING PigStorage(',') 
    AS (id:int, name:chararray, marks:int);

high_marks = FILTER students BY marks > 80;
grouped = GROUP students BY marks;
count_per_marks = FOREACH grouped GENERATE group, COUNT(students);

STORE high_marks INTO '/user/hadoop/output_high_marks' USING PigStorage(',');
STORE count_per_marks INTO '/user/hadoop/output_counts' USING PigStorage(',');
