package com.howell.spark;


import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class Action {
    public static void main(String[] args) {
        String[] jarList= {"/media/program/linux/work/git/github/spark-web/out/artifacts/spark_web_jar"};
        SparkConf sparkConf = new SparkConf()
                .setAppName("Action")
                .setMaster("spark://master:7077")
                .setJars(jarList)
                .set("spark.eventLog.dir", "hdfs://slaves1:9000/user/root/.sparkStaging")
                .set("spark.eventLog.enabled", "true");
        SparkContext sparkContext = new SparkContext(sparkConf);

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);

        JavaRDD<String> peopleRDD = javaSparkContext
                .textFile("/home/howell/works/spark-work/word.txt", 2);
        JavaRDD errors  = peopleRDD.filter(s -> s.contains("error"));

        System.out.println(errors.count());

        sparkContext.stop();

    }
}
