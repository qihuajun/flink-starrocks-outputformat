# StarRocksOutputFormat

StarRocks OutputFormat using StarRocks' `stream load` API for Flink DataSet output

## How to use

sample code

```java
StarRocksRowOutputFormat.buildOutputFormat()
                                .setUrl("127.0.0.1:8030")
                                .setUsername("test")
                                .setPassword("test")
                                .setBatchInterval(20000)
                                .setDatabase("test")
                                .setTable("test")
                                .setColumns(columns)
                                .finish()
```