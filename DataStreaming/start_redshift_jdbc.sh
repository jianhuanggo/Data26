curl -X POST -H "Accept:application/json" -H "Content-Type:application/json" af71d3110348a11e9960602a808c5d92-1853439397.us-west-2.elb.amazonaws.com:8083/connectors/ -d '{
 "name": "redshift-sink-2",
 "config": {
 "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
 "tasks.max": "1",
 "topics":"ptest_batteries",
 "connection.url": "jdbc:redshift://scoot-dw.cmajy1mmc4g4.us-west-1.redshift.amazonaws.com:5439/scootdw?user=scootdata&password=eVkbDgX1!HlJG1fk+Qwi",
 "errors.log.enable": "false",
 "auto.create": "false",
 "insert.mode": "insert"
 }
}'
