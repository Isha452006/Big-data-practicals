$scriptContent = @"
-- Sample Sqoop Import Command
sqoop import \
--connect jdbc:mysql://localhost:3306/school \
--username root --password password \
--table students \
--target-dir /user/hadoop/students \
--fields-terminated-by ','
"@
Set-Content -Path ".\Practical5_Sqoop_Flume\sample_sqoop_import.sh" -Value $scriptContent
