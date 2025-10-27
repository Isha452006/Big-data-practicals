$hbaseScript = @"
# Sample HBase shell commands
create 'students','info'
put 'students','1','info:name','Alice'
put 'students','1','info:score','50'
get 'students','1'
scan 'students'
delete 'students','1','info:score'
drop 'students'
"@
Set-Content -Path ".\Practical6_HBase_Zookeeper_Oozie\sample_hbase.sh" -Value $hbaseScript
