

```
/tools/spark/bin/spark-submit --master  yarn-cluster  --class  com.howell.spark.Mysql --driver-class-path /tools/jars/mysql-connector-java-5.1.38.jar  /tools/jars/spark-web-1.0-SNAPSHOT.jar
```


```
/tools/spark/bin/spark-submit --master  yarn-cluster  --class  com.howell.spark.ipcc.GoodsFormMysql --driver-class-path /tools/jars/mysql-connector-java-5.1.38.jar  /tools/jars/spark-web-1.0-SNAPSHOT.jar

/tools/spark/bin/spark-submit --master  yarn-cluster  --class  com.howell.spark.ipcc.GoodsItemNameByCategory --driver-class-path /tools/jars/mysql-connector-java-5.1.38.jar  /tools/jars/spark-web-1.0-SNAPSHOT.jar

/tools/spark/bin/spark-submit --master  yarn-cluster  --class  com.howell.spark.ipcc.GoodsItemNameByPosition --driver-class-path /tools/jars/mysql-connector-java-5.1.38.jar  /tools/jars/spark-web-1.0-SNAPSHOT.jar


```


```
/tools/install/spark/bin/spark-submit --master  yarn-cluster  --class  com.howell.spark.SparkPi  /tools/jars/spark-web-1.0-SNAPSHOT.jar
```


