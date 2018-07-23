package com.howell.spark;


import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class App {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("howell").master("spark://master:7077").getOrCreate();
        SparkContext sparkContext = sparkSession.sparkContext();

        // 创建一个RDD
        JavaRDD<String> peopleRDD = sparkContext
                .textFile("/home/howell/works/spark-work/word.txt", 2)
                .toJavaRDD();

        String schemaString = "name age";
        // 基于用字符串定义的schema生成StructType
        List<StructField> fields = new ArrayList();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);


// 把RDD (people)转换为Rows
        JavaRDD<Row> rowRDD = peopleRDD.map(record -> {
            String[] attributes = record.split(",");
            return RowFactory.create(attributes[0], attributes[1].trim());
        });


// 对RDD应用schema
        Dataset<Row> peopleDataFrame = sparkSession.createDataFrame(rowRDD, schema);

// 使用DataFrame创建临时视图
        peopleDataFrame.createOrReplaceTempView("people");

// 运行SQL查询
        Dataset<Row> results = sparkSession.sql("SELECT name FROM people");

// SQL查询的结果是DataFrames类型，支持所有一般的RDD操作
// 结果的列可以通过属性的下标或者名字获取
//        Dataset<String> namesDS = results.map(row -> "Name: " + row.getString(0), Encoders.STRING());
//        namesDS.show();

    }
}
