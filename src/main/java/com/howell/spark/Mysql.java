package com.howell.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDD;

import java.sql.*;
import java.util.*;

public class Mysql {
    public static Connection getConnection(){
        try {
            return  DriverManager.getConnection("jdbc:mysql://10.15.51.100:3306/spark","root","wtoip@2015");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }




    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("msyql");
        SparkContext sc = new SparkContext(conf);

        SparkSession sparkSession = new SparkSession(sc);

        List<Dbuser> list = new ArrayList<>();




        Connection connection = getConnection();
        PreparedStatement preparedStatement  = null;
        try {
            preparedStatement = connection.prepareStatement("select * from db_user" );
            ResultSet result= preparedStatement.executeQuery();
            while(result.next()){
                Dbuser a =new Dbuser();
                a.setId(result.getString("id"));
                a.setName(result.getString("name"));
                list.add(a);
            }
            Dataset<Row> df = sparkSession.createDataFrame(list, Dbuser.class);
            df.write().mode(SaveMode.Overwrite).csv("/user/root/Dbuser/user.csv");
            //df.write().mode(SaveMode.Overwrite).parquet("/user/root/Dbuser/user.bat");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        Dataset<Row> dataset = sparkSession.read().csv("/user/root/Dbuser/user.csv");

        dataset.show();

    }
}
