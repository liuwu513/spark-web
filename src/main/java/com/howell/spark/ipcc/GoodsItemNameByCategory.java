package com.howell.spark.ipcc;

import com.howell.spark.bean.RDDKeyByCounts;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.regex.Pattern;


public class GoodsItemNameByCategory {

    private static final Pattern SPACE = Pattern.compile("");
    private static final String regexStr = "[^\u4E00-\u9FA5]";  //匹配中文的正则表达式

    private static class TupleComparator implements Comparator<Tuple2<String, Integer>>, Serializable {
        @Override
        public int compare(Tuple2<String, Integer> tuple1, Tuple2<String, Integer>tuple2) {
            return tuple1._2 < tuple2._2 ? 0 : 1;
        }
    }

    public static void main(String[] args) {
        //自定义比较器
        SparkConf conf = new SparkConf().setAppName("wtoip_ipcc_goods");
        SparkContext sc = new SparkContext(conf);

        SparkSession sparkSession = new SparkSession(sc);

        Dataset<Row> goodsDF = sparkSession.read().format("json").json("/ipcc/wtoip_ipcc_goods/source.json");

        JavaRDD<Row> dataset = goodsDF.select("item_name").toJavaRDD();

        JavaRDD<String> words = dataset.flatMap(s -> Arrays.asList(SPACE.split(s.toString().replaceAll(regexStr, ""))).iterator());

        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);


        List<Tuple2<String, Integer>> output = counts.collect();

        output.sort(new TupleComparator());

        //List<Tuple2<String, Integer>> output = counts.sortByKey().collect();
        List<RDDKeyByCounts> list = new ArrayList<>();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
            RDDKeyByCounts keyByCounts = new RDDKeyByCounts();
            keyByCounts.setName(tuple._1().toString());
            keyByCounts.setCounts(tuple._2().toString());
            list.add(keyByCounts);
        }

        Dataset<Row> df = sparkSession.createDataFrame(list, RDDKeyByCounts.class);
        df.write().mode(SaveMode.Overwrite).json("/ipcc/wtoip_ipcc_goods/GoodsItemNameByCategory.json");
        sparkSession.stop();
        //dataset.show();

    }
}
