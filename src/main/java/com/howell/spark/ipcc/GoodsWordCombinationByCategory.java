package com.howell.spark.ipcc;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * 分类组合
 * Created by liuwu on 2018/7/30 0030.
 */
public class GoodsWordCombinationByCategory {

    private static final int minCounts = 10;

    public static void read2ByCategory(SparkContext sc, SparkSession sparkSession, String goods_category) {
        //第一位数据集合
        Dataset<Row> goodsDF0 = sparkSession.read().format("json").json("/ipcc/wtoip_ipcc_goods/all/category.json");

        //第二位数据集合
        Dataset<Row> goodsDF1 = sparkSession.read().format("json").json(String.format("/ipcc/wtoip_ipcc_goods/%s/%s/position.json", 1, 2));
        //过滤数据集合
        Dataset<Row> filterDF1 = goodsDF1.selectExpr("name as secondName", "counts as secondCounts").where(String.format("counts >= %d", minCounts));

        Dataset<Row> combination2DF = goodsDF0.crossJoin(filterDF1).selectExpr("concat(name,secondName) as name", "(counts + secondCounts) as counts");

        //移除重复值
        Dataset<Row> distinctCombination2DF = combination2DF.dropDuplicates("name");

        distinctCombination2DF.write().mode(SaveMode.Overwrite).json("/ipcc/wtoip_ipcc_goods/1/combination2.json");

        //拼装 1,2,3位
        read3ByCategory(sc, sparkSession, goods_category);
    }

    public static void read3ByCategory(SparkContext sc, SparkSession sparkSession, String goods_category) {
        //1和2组合
        Dataset<Row> goodsDF1 = sparkSession.read().format("json").json(String.format("/ipcc/wtoip_ipcc_goods/%s/combination2.json", goods_category));
        //第三位数据集合
        Dataset<Row> goodsDF2 = sparkSession.read().format("json").json(String.format("/ipcc/wtoip_ipcc_goods/%s/%s/position.json", 1, 3));
        //过滤数据集合
        Dataset<Row> filterDF2 = goodsDF2.selectExpr("name as secondName", "counts as secondCounts").where(String.format("counts >= %d", minCounts));
        //1、2、3位组合
        Dataset<Row> combination3DF = goodsDF1.crossJoin(filterDF2).selectExpr("concat(name,secondName) as combination2Name", "(counts + secondCounts) as count2Total");

        //移除重复值
        Dataset<Row> distinctCombination3DF = combination3DF.dropDuplicates("name");

        distinctCombination3DF.write().mode(SaveMode.Overwrite).json(String.format("/ipcc/wtoip_ipcc_goods/%s/combination3.json", goods_category));

        //拼装 1,2,3,4位
        read4ByCategory(sc, sparkSession, goods_category);
    }

    public static void read4ByCategory(SparkContext sc, SparkSession sparkSession, String goods_category) {
        //1、2、3组合
        Dataset<Row> goodsDF1 = sparkSession.read().format("json").json(String.format("/ipcc/wtoip_ipcc_goods/%s/combination3.json", goods_category));
        //第4位数据集合
        Dataset<Row> goodsDF2 = sparkSession.read().format("json").json(String.format("/ipcc/wtoip_ipcc_goods/%s/%s/position.json", 1, 4));
        //过滤数据集合
        Dataset<Row> filterDF2 = goodsDF2.selectExpr("name as secondName", "counts as secondCounts").where(String.format("counts >= %d", minCounts));
        //1、2、3、4位组合
        Dataset<Row> combination3DF = goodsDF1.crossJoin(filterDF2).selectExpr("concat(name,secondName) as combination2Name", "(counts + secondCounts) as count2Total");

        //移除重复值
        Dataset<Row> distinctCombination3DF = combination3DF.dropDuplicates("name");

        distinctCombination3DF.write().mode(SaveMode.Overwrite).json(String.format("/ipcc/wtoip_ipcc_goods/%s/combination4.json", goods_category));
    }


    public static void main(String[] args) {
        //自定义比较器
        SparkConf conf = new SparkConf().setAppName("wtoip_ipcc_goods");
        SparkContext sc = new SparkContext(conf);

        SparkSession sparkSession = new SparkSession(sc);
        for (int i = 0; i <= 45; i++) {
            read2ByCategory(sc, sparkSession, String.valueOf(i));
        }
        sparkSession.stop();
    }
}
