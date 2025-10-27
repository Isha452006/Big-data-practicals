$readmeContent = @"
# Practical 5: Data Ingestion using Sqoop and Flume

## Objective
Transfer data between RDBMS, Hadoop, and streaming sources.

## Tasks
- Import data from MySQL to HDFS using Sqoop.
- Export processed data from HDFS back to MySQL.
- Configure Flume agent to collect log or streaming data.
- Transfer data to HDFS or HBase in real time.

## Outcome
Students understand data movement and ingestion in Hadoop environments.

## Tools Used
- Apache Sqoop
- Apache Flume
- Hadoop
- MySQL
"@
Set-Content -Path ".\Practical5_Sqoop_Flume\README.md" -Value $readmeContent
