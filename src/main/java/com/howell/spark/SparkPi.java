package com.howell.spark;

import com.howell.spark.bean.DB_User;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;


import java.util.ArrayList;
import java.util.List;

public class SparkPi {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Howell");
        SparkContext sparkContext = new SparkContext(sparkConf);
        SparkSession sparkSession = new SparkSession(sparkContext);



        List<DB_User> list = new  ArrayList<>();

        DB_User db_user = new DB_User();
        db_user.setId(1);
        db_user.setName("HowellYang");
        db_user.setAge(14);
        db_user.setSex(1);
        db_user.setCid("44051234234245334433");
        list.add(db_user);
        db_user = new DB_User();
        db_user.setId(1);
        db_user.setName("YangHaiHua");
        db_user.setAge(14);
        db_user.setSex(1);
        db_user.setCid("540034513451345433344");
        list.add(db_user);


        Dataset<Row>  db = sparkSession.createDataFrame(list, DB_User.class);
        db.write().mode(SaveMode.Overwrite).json("/user/SparkPi/user.json");


       // sparkSession.read().json("/user/SparkPi/user.json").flatMap()


    }
}
