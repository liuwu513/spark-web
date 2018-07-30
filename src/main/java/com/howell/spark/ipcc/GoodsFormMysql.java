package com.howell.spark.ipcc;

import com.howell.spark.bean.DB_ipcc_goods;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class GoodsFormMysql {
    public static Connection getConnection(){
        try {
            return  DriverManager.getConnection("jdbc:mysql://10.15.51.100:3306/wtoip_ipcc_goods_prod","root","wtoip@2015");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }


    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("wtoip_ipcc_goods");
        SparkContext sc = new SparkContext(conf);

        SparkSession sparkSession = new SparkSession(sc);

        List<DB_ipcc_goods> list = new ArrayList<>();

        Connection connection = getConnection();
        PreparedStatement preparedStatement  = null;
        try {
            preparedStatement = connection.prepareStatement("SELECT a.item_name, a.goods_category FROM ipcc_goods a WHERE a.dict_type='100203'" );
            ResultSet result= preparedStatement.executeQuery();
            while(result.next()){
                DB_ipcc_goods ipcc_goods =new DB_ipcc_goods();
                ipcc_goods.setItem_name(result.getString("item_name"));
                ipcc_goods.setGoods_category(result.getString("goods_category"));
                list.add(ipcc_goods);
            }
            Dataset<Row> df = sparkSession.createDataFrame(list, DB_ipcc_goods.class);
            df.write().mode(SaveMode.Overwrite).json("/ipcc/wtoip_ipcc_goods/source.json");

        } catch (SQLException e) {
            e.printStackTrace();
        }

        //Dataset<Row> dataset = sparkSession.read().csv("/user/root/Dbuser/user.csv");

        //dataset.show();

    }
}
