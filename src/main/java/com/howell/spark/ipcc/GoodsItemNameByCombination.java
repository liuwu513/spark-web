package com.howell.spark.ipcc;

import com.howell.spark.bean.DB_ipcc_goods;
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


    /**
     * 组合
     * @param sc
     * @param sparkSession
     */
    public static void readAll(SparkContext sc, SparkSession sparkSession){
        Dataset<Row> goodsDF = sparkSession.read().format("json").json("/ipcc/wtoip_ipcc_goods/all/category.json");
        List<Row> list = goodsDF.select("name").toJavaRDD().collect();
        List<DB_ipcc_goods> dbIpccGoodsList = new ArrayList<>();
        for (int i=0; i<list.size();i++){
            Row iRow = list.get(i);
            for (int j=0; j<list.size();j++){
                Row jRow = list.get(j);
                DB_ipcc_goods jdb_ipcc_goods = new DB_ipcc_goods();
                if(!iRow.getString(0).equals(jRow.getString(0))){
                    String word = iRow.getString(0)+jRow.getString(0);
                    jdb_ipcc_goods.setItem_name(word);
                    jdb_ipcc_goods.setGoods_category("all");
                    dbIpccGoodsList.add(jdb_ipcc_goods);
                }
                for(int x=0;x<list.size();x++){
                    Row xRow = list.get(x);
                    DB_ipcc_goods xdb_ipcc_goods = new DB_ipcc_goods();
                    if(!iRow.getString(0).equals(jRow.getString(0)) && !iRow.getString(0).equals(xRow.getString(0)) && !jRow.getString(0).equals(xRow.getString(0))){
                        String word = iRow.getString(0)+jRow.getString(0)+xRow.getString(0);
                        xdb_ipcc_goods.setItem_name(word);
                        xdb_ipcc_goods.setGoods_category("all");
                        dbIpccGoodsList.add(xdb_ipcc_goods);
                    }
                    for(int y=0;y<list.size();y++){
                        Row yRow = list.get(y);
                        DB_ipcc_goods ydb_ipcc_goods = new DB_ipcc_goods();
                        if(!iRow.getString(0).equals(jRow.getString(0))
                                && !iRow.getString(0).equals(xRow.getString(0))
                                && !iRow.getString(0).equals(yRow.getString(0))
                                && !jRow.getString(0).equals(xRow.getString(0))
                                && !jRow.getString(0).equals(yRow.getString(0))
                                && !xRow.getString(0).equals(yRow.getString(0))
                                ){
                            String word = iRow.getString(0)+jRow.getString(0)+xRow.getString(0)+yRow.getString(0);
                            ydb_ipcc_goods.setItem_name(word);
                            ydb_ipcc_goods.setGoods_category("all");
                            dbIpccGoodsList.add(ydb_ipcc_goods);
                        }
                    }
                }
            }
        }

        Dataset<Row> df = sparkSession.createDataFrame(dbIpccGoodsList, DB_ipcc_goods.class);
        df.write().mode(SaveMode.Overwrite).json("/ipcc/wtoip_ipcc_goods/all.json");
    }

    public static void main(String[] args) {
        //自定义比较器
        SparkConf conf = new SparkConf().setAppName("wtoip_ipcc_goods");
        SparkContext sc = new SparkContext(conf);

        SparkSession sparkSession = new SparkSession(sc);
        readAll(sc, sparkSession);

//        for (int i=1; i<=45; i++){
//            for(int j=2; j <=4; j++) {
//                read(String.valueOf(i), sc, sparkSession, j);
//            }
//        }
        sparkSession.stop();
    }
}
