package com.howell.spark.ipcc;

import com.howell.spark.bean.RDDKeyByCounts;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 组合
 */
public class GoodsItemNameByCombination {

    private static final Pattern SPACE = Pattern.compile("");
    private static final String regexStr = "[^\u4E00-\u9FA5]";  //匹配中文的正则表达式



    public static void read(String goods_category, SparkContext sc, SparkSession sparkSession, int position){
        Dataset<Row> goodsDF = sparkSession.read().format("json").json("/ipcc/wtoip_ipcc_goods/"+goods_category +"/category.json");
        List<Row> list = goodsDF.collectAsList();
        for (int i=0; i<list.size();i++){

        }

    }

    public static void main(String[] args) {
        //自定义比较器
        SparkConf conf = new SparkConf().setAppName("wtoip_ipcc_goods");
        SparkContext sc = new SparkContext(conf);

        SparkSession sparkSession = new SparkSession(sc);

        for (int i=1; i<=45; i++){
            for(int j=2; j <=4; j++) {
                read(String.valueOf(i), sc, sparkSession, j);
            }
        }
        sparkSession.stop();
    }
}
