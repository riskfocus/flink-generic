# Flink JDBC Sink module

Module provides JDBC Sink function.


Original [Flink JDBC connector](https://github.com/apache/flink-connector-jdbc) has a nasty bug which prevents of usage bulk inserts ([see details](https://issues.apache.org/jira/browse/FLINK-17488))

- [Sink parameters configuration](src/main/java/com/ness/flink/sink/jdbc/properties/JdbcSinkProperties.java)

There are two options how you could Sink your data to Database 

1. By using JDBC Sink Usage
   ([Example of usage](src/test/java/com/ness/flink/sink/jdbc/JdbcSinkIT.java))

2. By using JDBC KeyedProcessor Usage
   ([Example of usage](src/test/java/com/ness/flink/sink/jdbc/KeyedJdbcProcessFunctionIT.java))
