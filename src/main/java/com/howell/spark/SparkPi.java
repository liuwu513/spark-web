package com.howell.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

public class SparkPi {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        String[] strings = {"/media/program/linux/work/git/github/spark-web/out/artifacts/spark_web_jar"};
        sparkConf.setAppName("Howell").setMaster("spark://master:7077").setJars(strings);
        SparkContext sparkContext = new SparkContext(sparkConf);

        sparkContext.textFile("/home/howell/works/spark-work/word.txt", 2);
    }
}
