curl -X POST -H "Accept:application/json" -H "Content-Type:application/json" af71d3110348a11e9960602a808c5d92-1853439397.us-west-2.elb.amazonaws.com:8083/connectors/ -d '{
 "name": "postgres-jdbc-source-7",
 "config": {
 "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
 "tasks.max": "1",
 "connection.url": "jdbc:postgresql://oregon-read-only.ccfrcisbix7u.us-west-1.rds.amazonaws.com:5432/ddh3j8703l2puv?user=u3jp0qoj4h99rj&password=p15d5eh2gahoo6ck2gn6qiocavp",
 "mode": "incrementing",
 "incrementing.column.name": "id",
 "timestamp.column.name": "updated_at",
 "topic.prefix": "ptest_",
 "table.whitelist": "batteries"
 }
}'
