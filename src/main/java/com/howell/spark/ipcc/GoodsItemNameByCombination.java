package com.howell.spark.ipcc;

import com.howell.spark.bean.RDDKeyByCounts;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.Collections;
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
        List<Row> list = goodsDF.select("name","counts").toJavaRDD().collect();
        List<RDDKeyByCounts> idbIpccGoodsList = new ArrayList<>();
        for (int i=0; i<list.size();i++) {
            Row iRow = list.get(i);
            for (int j = 0; j < list.size(); j++) {
                Row jRow = list.get(j);
                if (!iRow.getString(0).equals(jRow.getString(0))) {
                    RDDKeyByCounts jRDDKeyByCounts = new RDDKeyByCounts();
                    String word = iRow.getString(0) + jRow.getString(0);
                    jRDDKeyByCounts.setName(word);
                    jRDDKeyByCounts.setCounts(Integer.parseInt(iRow.getString(1)) + Integer.parseInt(jRow.getString(1)));
                    jRDDKeyByCounts.setGoods_category("all");
                    idbIpccGoodsList.add(jRDDKeyByCounts);
                }
            }
        }

        Encoder<RDDKeyByCounts> countsEncoder = Encoders.bean(RDDKeyByCounts.class);
        Dataset<RDDKeyByCounts> javaBeanDS = sparkSession.createDataset(
                idbIpccGoodsList,
                countsEncoder
        );

        JavaRDD<RDDKeyByCounts> javaRDD = javaBeanDS.toJavaRDD().sortBy(new Function<RDDKeyByCounts, Integer>(){
            @Override
            public Integer call(RDDKeyByCounts rddKeyByCounts) throws Exception {
                return rddKeyByCounts.getCounts();
            }
        },false, 3);

        javaBeanDS = sparkSession.createDataset(javaRDD.rdd(), countsEncoder);
        javaBeanDS.write().mode(SaveMode.Overwrite).json("/ipcc/wtoip_ipcc_goods/all2.json");




//        List<Row> list = goodsDF.select("name","counts").toJavaRDD().collect();
//        List<RDDKeyByCounts> idbIpccGoodsList = new ArrayList<>();
//        List<RDDKeyByCounts> jdbIpccGoodsList = new ArrayList<>();
//        List<RDDKeyByCounts> ydbIpccGoodsList = new ArrayList<>();
//        for (int i=0; i<list.size();i++){
//            Row iRow = list.get(i);
//            for (int j=0; j<list.size();j++){
//                Row jRow = list.get(j);
//                RDDKeyByCounts jRDDKeyByCounts = new RDDKeyByCounts();
//                if(!iRow.getString(0).equals(jRow.getString(0))){
//                    String word = iRow.getString(0)+jRow.getString(0);
//                    jRDDKeyByCounts.setName(word);
//                    jRDDKeyByCounts.setCounts(String.valueOf( Integer.parseInt(iRow.getString(1)) + Integer.parseInt(jRow.getString(1)) ));
//                    jRDDKeyByCounts.setGoods_category("all");
//                    idbIpccGoodsList.add(jRDDKeyByCounts);
//                }
//                for(int x=0;x<list.size();x++){
//                    Row xRow = list.get(x);
//                    RDDKeyByCounts xRDDKeyByCounts = new RDDKeyByCounts();
//                    if(!iRow.getString(0).equals(jRow.getString(0)) && !iRow.getString(0).equals(xRow.getString(0)) && !jRow.getString(0).equals(xRow.getString(0))){
//                        String word = iRow.getString(0)+jRow.getString(0)+xRow.getString(0);
//                        xRDDKeyByCounts.setName(word);
//                        jRDDKeyByCounts.setCounts(String.valueOf( Integer.parseInt(iRow.getString(1)) + Integer.parseInt(jRow.getString(1)) + Integer.parseInt(xRow.getString(1)) ));
//                        xRDDKeyByCounts.setGoods_category("all");
//                        jdbIpccGoodsList.add(xRDDKeyByCounts);
//                    }
//                    for(int y=0;y<list.size();y++){
//                        Row yRow = list.get(y);
//                        RDDKeyByCounts yRDDKeyByCounts = new RDDKeyByCounts();
//                        if(!iRow.getString(0).equals(jRow.getString(0))
//                                && !iRow.getString(0).equals(xRow.getString(0))
//                                && !iRow.getString(0).equals(yRow.getString(0))
//                                && !jRow.getString(0).equals(xRow.getString(0))
//                                && !jRow.getString(0).equals(yRow.getString(0))
//                                && !xRow.getString(0).equals(yRow.getString(0))
//                                ){
//                            String word = iRow.getString(0)+jRow.getString(0)+xRow.getString(0)+yRow.getString(0);
//                            yRDDKeyByCounts.setName(word);
//                            yRDDKeyByCounts.setGoods_category("all");
//                            ydbIpccGoodsList.add(yRDDKeyByCounts);
//                        }
//                    }
//                }
//            }
//        }

//        Dataset<Row> df = sparkSession.createDataFrame(idbIpccGoodsList, RDDKeyByCounts.class);
//        df.write().mode(SaveMode.Overwrite).json("/ipcc/wtoip_ipcc_goods/all2.json");
//        df = sparkSession.createDataFrame(jdbIpccGoodsList, RDDKeyByCounts.class);
//        df.write().mode(SaveMode.Overwrite).json("/ipcc/wtoip_ipcc_goods/all3.json");
//        df = sparkSession.createDataFrame(ydbIpccGoodsList, RDDKeyByCounts.class);
//        df.write().mode(SaveMode.Overwrite).json("/ipcc/wtoip_ipcc_goods/all4.json");
    }



    /**
     * 组合4
     * @param sc
     * @param sparkSession
     */
    public static void readAll4(SparkContext sc, SparkSession sparkSession) {
        Dataset<Row> goodsDF = sparkSession.read().format("json").json("/ipcc/wtoip_ipcc_goods/all/category.json");
        List<Row> list = goodsDF.select("name", "counts").toJavaRDD().collect();
        Encoder<RDDKeyByCounts> countsEncoder = Encoders.bean(RDDKeyByCounts.class);
        Dataset<RDDKeyByCounts> javaBeanDS = null;
        JavaRDD<RDDKeyByCounts> javaRDD = null;
          for (int i=0; i<list.size();i++){
            Row iRow = list.get(i);
            for (int j=0; j<list.size();j++){
                Row jRow = list.get(j);
                List<RDDKeyByCounts> idbIpccGoodsList = new ArrayList<>();
                for(int x=0;x<list.size();x++){
                    Row xRow = list.get(x);
                    for(int y=0;y<list.size();y++){
                        Row yRow = list.get(y);
                        RDDKeyByCounts yRDDKeyByCounts = new RDDKeyByCounts();
                        if(!iRow.getString(0).equals(jRow.getString(0))
                                && !iRow.getString(0).equals(xRow.getString(0))
                                && !iRow.getString(0).equals(yRow.getString(0))
                                && !jRow.getString(0).equals(xRow.getString(0))
                                && !jRow.getString(0).equals(yRow.getString(0))
                                && !xRow.getString(0).equals(yRow.getString(0))
                                ){

                            String word = iRow.getString(0) + jRow.getString(0) + xRow.getString(0) + yRow.getString(0);
                            yRDDKeyByCounts.setName(word);
                            yRDDKeyByCounts.setCounts(Integer.parseInt(iRow.getString(1)) + Integer.parseInt(jRow.getString(1)) + Integer.parseInt(xRow.getString(1)) + Integer.parseInt(yRow.getString(1)));
                            yRDDKeyByCounts.setGoods_category("all");
                            idbIpccGoodsList.add(yRDDKeyByCounts);
                        }
                    }
                }
                javaBeanDS = sparkSession.createDataset(
                        idbIpccGoodsList,
                        countsEncoder
                );

                javaRDD = javaBeanDS.toJavaRDD().sortBy(new Function<RDDKeyByCounts, Integer>() {
                    @Override
                    public Integer call(RDDKeyByCounts rddKeyByCounts) throws Exception {
                        return rddKeyByCounts.getCounts();
                    }
                }, false, 3);

                javaBeanDS = sparkSession.createDataset(javaRDD.rdd(), countsEncoder);
                javaBeanDS.write().mode(SaveMode.Overwrite).json("/ipcc/wtoip_ipcc_goods/all_4/"+i+"_"+j+".json");
            }
        }

    }
    public static void main(String[] args) {
        //自定义比较器
        SparkConf conf = new SparkConf().setAppName("wtoip_ipcc_goods");
        SparkContext sc = new SparkContext(conf);

        SparkSession sparkSession = new SparkSession(sc);
        readAll4(sc, sparkSession);

//        for (int i=1; i<=45; i++){
//            for(int j=2; j <=4; j++) {
//                read(String.valueOf(i), sc, sparkSession, j);
//            }
//        }
        sparkSession.stop();
    }
}
