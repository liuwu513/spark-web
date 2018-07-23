package com.howell.spark;


import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;


public class Action {
    public static void main(String[] args) {
        String[] jarList= {"/media/program/linux/work/git/github/spark-web/out/artifacts/spark_web_jar"};
        SparkConf sparkConf = new SparkConf()
                .setAppName("Action")
                .setMaster("spark://master:7077")
                .set("spark.eventLog.dir", "hdfs://slaves1:9000/user/root/.sparkStaging")
                .set("spark.eventLog.enabled", "true");
        SparkContext sparkContext = new SparkContext(sparkConf);

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);

        javaSparkContext.addFile("/home/howell/works/spark-work/word.txt");
        javaSparkContext.textFile("/home/howell/works/spark-work/word.txt");

        // 创建一个RDD
        JavaRDD<String> peopleRDD = sparkContext
                .textFile("/home/howell/works/spark-work/word.txt", 9)
                .toJavaRDD();

// 把RDD (people)转换为Rows
        JavaRDD<Row> rowRDD = peopleRDD.map(record -> {
            String[] attributes = record.split(",");
            System.out.println(attributes[0]);
            return RowFactory.create(attributes[0], attributes[1].trim());
        });




        sparkContext.stop();

    }
}
