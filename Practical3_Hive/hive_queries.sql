-- Create an internal table
CREATE TABLE IF NOT EXISTS students (
    id INT,
    name STRING,
    marks INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Load data from HDFS into the table
LOAD DATA INPATH '/user/hadoop/students.csv' INTO TABLE students;

-- Example queries
-- Select all students
SELECT * FROM students;

-- Select students with marks greater than 80
SELECT name, marks FROM students WHERE marks > 80;

-- Count total students
SELECT COUNT(*) FROM students;

-- Average marks
SELECT AVG(marks) FROM students;

-- Group by example
SELECT marks, COUNT(*) FROM students GROUP BY marks;

-- Order by example
SELECT * FROM students ORDER BY marks DESC;

