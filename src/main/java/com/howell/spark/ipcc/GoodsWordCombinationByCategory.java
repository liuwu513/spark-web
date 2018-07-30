package com.howell.spark.ipcc;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 分类组合
 * Created by liuwu on 2018/7/30 0030.
 */
public class GoodsWordCombinationByCategory {

    private static final int minCounts = 10;

    public static void readByCategory(SparkContext sc, SparkSession sparkSession, String goods_category) {
        //第二位数据集合
        Dataset<Row> goodsDF1 = sparkSession.read().format("json").json(String.format("/ipcc/wtoip_ipcc_goods/%s/%s/position.json",1,2));
        //过滤数据集合
        Dataset<Row> filterDF1 = goodsDF1.select("name","counts").where(String.format("counts >= %d",minCounts));
        //创建临时表
//        filterDF1.createOrReplaceTempView("goods_category_1");


        //第三位数据集合
        Dataset<Row> goodsDF2 = sparkSession.read().format("json").json(String.format("/ipcc/wtoip_ipcc_goods/%s/%s/position.json",1,3));
        //过滤数据集合
        Dataset<Row> filterDF2 = goodsDF2.selectExpr("name as secondName","counts as secondCounts").where(String.format("counts >= %d",minCounts));

        //合并第二字和第三字
        Dataset<Row> combination2DF = filterDF1.union(filterDF2).selectExpr("concat(name,secondName) as combination2Name","(counts + secondCounts) as count2Total");
        //显示前10行
        combination2DF.show(10);

        //创建临时表
//        filterDF2.createOrReplaceTempView("goods_category_2");

        //第四位数据集合
//        Dataset<Row> goodsDF3 = sparkSession.read().format("json").json(String.format("/ipcc/wtoip_ipcc_goods/%s/%s/position.json",1,4));
//        Dataset<Row> filterDF3 = goodsDF2.selectExpr("name as thirdName","counts as thirdCounts").where(String.format("counts >= %d",minCounts));
        //创建临时表
//        filterDF3.createOrReplaceTempView("goods_category_3");
    }

    public static void main(String[] args) {
        //自定义比较器
        SparkConf conf = new SparkConf().setAppName("wtoip_ipcc_goods");
        SparkContext sc = new SparkContext(conf);

        SparkSession sparkSession = new SparkSession(sc);

        readByCategory(sc, sparkSession, "1");

        sparkSession.stop();
    }
}
