package com.howell.spark.ipcc;

import com.howell.spark.bean.RDDKeyByCounts;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import scala.Function1;
import scala.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 组合
 */
public class GoodsWordCombination {

    private static final Pattern SPACE = Pattern.compile("");
    private static final String regexStr = "[^\u4E00-\u9FA5]";  //匹配中文的正则表达式


    /**
     * 组合
     * @param sc
     * @param sparkSession
     */
    public static void read2(SparkContext sc, SparkSession sparkSession){
        Dataset<Row> goodsDF1 = sparkSession.read().format("json").json("/ipcc/wtoip_ipcc_goods/all/category.json");
        JavaRDD<Row> rdd1 = goodsDF1.select("name", "counts").toJavaRDD();
        JavaRDD<Row> rdd2 = goodsDF1.select("name", "counts").toJavaRDD();
        List<Tuple2<Row, Row>> output = rdd1.cartesian(rdd2).collect();
        JavaSparkContext jsc = new JavaSparkContext(sc);
        JavaRDD<Tuple2<Row, Row>> tuple2JavaRDD = jsc.parallelize(output);
        // 排序
        tuple2JavaRDD = tuple2JavaRDD.sortBy(new Function<Tuple2<Row, Row>, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Integer call(Tuple2<Row, Row> v )  {
                return Integer.parseInt(v._1().get(1).toString())+Integer.parseInt(v._2().get(1).toString());
            }
        }, false, 3);
        output =  tuple2JavaRDD.collect();

        List<RDDKeyByCounts> list = new ArrayList<>();
        for (int i=0; i<output.size(); i++) {
            Tuple2<Row,Row> tuple = output.get(i);
            RDDKeyByCounts keyByCounts = new RDDKeyByCounts();
            keyByCounts.setName(tuple._1().getString(0)+tuple._2().getString(0));
            keyByCounts.setCounts(Integer.parseInt(tuple._1().get(1).toString())+Integer.parseInt(tuple._2().get(1).toString()));
            if(!tuple._1().getString(0).contains(tuple._2().getString(0))){
                list.add(keyByCounts);
            }
        }
        Dataset<Row> df = sparkSession.createDataFrame(list,  RDDKeyByCounts.class);
        df.write().mode(SaveMode.Overwrite).json("/ipcc/wtoip_ipcc_goods/all/combination2.json");
        read3(sc, sparkSession);
    }

    /**
     * 组合
     * @param sc
     * @param sparkSession
     */
    public static void read3(SparkContext sc, SparkSession sparkSession){
        Dataset<Row> goodsDF1 = sparkSession.read().format("json").json("/ipcc/wtoip_ipcc_goods/all/category.json");
        JavaRDD<Row> rdd1 = goodsDF1.select("name", "counts").toJavaRDD();
        Dataset<Row> goodsDF2 = sparkSession.read().format("json").json("/ipcc/wtoip_ipcc_goods/all/combination2.json");
        JavaRDD<Row> rdd2 = goodsDF2.select("name", "counts").toJavaRDD();
        List<Tuple2<Row, Row>> output = rdd1.cartesian(rdd2).collect();
        JavaSparkContext jsc = new JavaSparkContext(sc);
        JavaRDD<Tuple2<Row, Row>> tuple2JavaRDD = jsc.parallelize(output);
        // 排序
        tuple2JavaRDD = tuple2JavaRDD.sortBy(new Function<Tuple2<Row, Row>, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Integer call(Tuple2<Row, Row> v )  {
                return Integer.parseInt(v._1().get(1).toString())+Integer.parseInt(v._2().get(1).toString());
            }
        }, false, 3);
        output =  tuple2JavaRDD.collect();
        List<RDDKeyByCounts> list = new ArrayList<>();
        for (int i=0; i<output.size(); i++) {
            Tuple2<Row,Row> tuple = output.get(i);
            RDDKeyByCounts keyByCounts = new RDDKeyByCounts();
            keyByCounts.setName(tuple._1().getString(0)+tuple._2().getString(0));
            keyByCounts.setCounts(Integer.parseInt(tuple._1().get(1).toString())+Integer.parseInt(tuple._2().get(1).toString()));
            if(!tuple._2().getString(0).contains(tuple._1().getString(0))){
                list.add(keyByCounts);
            }
        }
        Dataset<Row> df = sparkSession.createDataFrame(list,  RDDKeyByCounts.class);
        df.write().mode(SaveMode.Overwrite).json("/ipcc/wtoip_ipcc_goods/all/combination3.json");
        read4(sc, sparkSession);
    }

    /**
     * 组合
     * @param sc
     * @param sparkSession
     */
    public static void read4(SparkContext sc, SparkSession sparkSession){
        Dataset<Row> goodsDF1 = sparkSession.read().format("json").json("/ipcc/wtoip_ipcc_goods/all/category.json");
        JavaRDD<Row> rdd1 = goodsDF1.select("name", "counts").toJavaRDD();
        Dataset<Row> goodsDF2 = sparkSession.read().format("json").json("/ipcc/wtoip_ipcc_goods/all/combination3.json");
        JavaRDD<Row> rdd2 = goodsDF2.select("name", "counts").toJavaRDD();
        List<Tuple2<Row, Row>> output = rdd1.cartesian(rdd2).collect();

        JavaSparkContext jsc = new JavaSparkContext(sc);
        JavaRDD<Tuple2<Row, Row>> tuple2JavaRDD = jsc.parallelize(output);
        // 排序
        tuple2JavaRDD = tuple2JavaRDD.sortBy(new Function<Tuple2<Row, Row>, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Integer call(Tuple2<Row, Row> v )  {
                return Integer.parseInt(v._1().get(1).toString())+Integer.parseInt(v._2().get(1).toString());
            }
        }, false, 3);
        output =  tuple2JavaRDD.collect();

        List<RDDKeyByCounts> list = new ArrayList<>();
        int file_item=0;
        for (int i=0; i<output.size(); i++) {
            Tuple2<Row,Row> tuple = output.get(i);
            RDDKeyByCounts keyByCounts = new RDDKeyByCounts();
            keyByCounts.setName(tuple._1().getString(0)+tuple._2().getString(0));
            keyByCounts.setCounts(Integer.parseInt(tuple._1().get(1).toString())+Integer.parseInt(tuple._2().get(1).toString()));
            if(!tuple._2().getString(0).contains(tuple._1().getString(0))){
                list.add(keyByCounts);
            }
            if(i%50000==0){
                file_item +=1;
                Dataset<Row> df = sparkSession.createDataFrame(list,  RDDKeyByCounts.class);
                df.write().mode(SaveMode.Overwrite).json("/ipcc/wtoip_ipcc_goods/all/combination4.json/"+file_item+".json");
                list = new ArrayList<>();
            }
        }
        Dataset<Row> df = sparkSession.createDataFrame(list,  RDDKeyByCounts.class);
        df.write().mode(SaveMode.Overwrite).json("/ipcc/wtoip_ipcc_goods/all/combination4.json/"+file_item+".json");
    }

    public static void main(String[] args) {
        //自定义比较器
        SparkConf conf = new SparkConf().setAppName("wtoip_ipcc_goods");
        SparkContext sc = new SparkContext(conf);

        SparkSession sparkSession = new SparkSession(sc);
        read2(sc, sparkSession);

        sparkSession.stop();
    }
}
