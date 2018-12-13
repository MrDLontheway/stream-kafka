package com.wxstc.dl.sparkj;

import com.wxstc.dl.School_count;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class WordCount {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[*]").appName("Test").getOrCreate();
        final JavaSparkContext ctx = JavaSparkContext.fromSparkContext(spark.sparkContext());

//        SparkConf conf = new SparkConf().setAppName("Test").setMaster("local[*]");
//		conf.set("spark.driver.extraClassPath", "/data/ojdbc14-10.2.0.3.0.jar");
//        conf.set("spark.executor.extraClassPath", "/data/ojdbc14-10.2.0.3.0.jar");
//        JavaSparkContext sparkContext = new JavaSparkContext(conf);

//        sparkContext.hadoopConfiguration().set("fs.defaultFS", "hdfs://mycluster");
//        sparkContext.hadoopConfiguration().set("dfs.nameservices", "mycluster");
//        sparkContext.hadoopConfiguration().set("dfs.ha.namenodes.mycluster", "nn1,nn2");
//        sparkContext.hadoopConfiguration().set("dfs.namenode.rpc-address.mycluster.nn1", "xx.xx.xx.xxx:8020");
//        sparkContext.hadoopConfiguration().set("dfs.namenode.rpc-address.mycluster.nn2", "xx.xx.xx.xxx:8020");
//        sparkContext.hadoopConfiguration().set("dfs.client.failover.proxy.provider.mycluster","org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        SQLContext sqlContext = spark.sqlContext();
        Map<String, String> options = new HashMap<String, String>();
        options.put("url", "jdbc:mysql://47.94.196.195:3306/bigdata?useUnicode=true&characterEncoding=UTF-8");
        options.put("user", "root");
        options.put("password", "dl575153");
        options.put("dbtable", "school_count");
        options.put("driver", "com.mysql.jdbc.Driver");

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "dl575153");
        connectionProperties.put("driver", "com.mysql.jdbc.Driver");
        txtfile(spark.sparkContext(),spark);

//        sqlContext.read().jdbc(options.get("url"), options.get("dbtable"), connectionProperties).createOrReplaceTempView("school");
//
//        Dataset jd2 = sqlContext.sql("SELECT * FROM school limit 10");
//
//        Dataset<Row> jdbcdf = sqlContext.read().format("jdbc").options(options).load();


//        JavaRDD<School_count> jdsc1 = jdbcdf.toJavaRDD().map(row -> {
//            School_count count = new School_count();
//            count.setId(row.getInt(0));
//            count.setNumber(row.getLong(1));
//            count.setSchoolName(row.getString(2));
//            count.setProvince(row.getString(3));
//            count.setLevel(row.getString(4));
//            count.setSchoolnature(row.getString(5));
//            count.setGuanwang(row.getString(6));
//            return count;
//        });
//
//        JavaRDD<School_count> jdsc = jdbcdf.toJavaRDD().map((Function<Row, School_count>) r -> {
//            School_count count = new School_count();
//            count.setId(r.getInt(0));
//            count.setNumber(r.getLong(1));
//            count.setSchoolName(r.getString(2));
//            count.setProvince(r.getString(3));
//            count.setLevel(r.getString(4));
//            count.setSchoolnature(r.getString(5));
//            count.setGuanwang(r.getString(6));
//            return count;
//        });
//        JavaRDD<School_count> jd = jdbcdf.map(new MapFunction<Row, School_count>() {
//                public School_count call(Row r) throws Exception {
//                School_count count = new School_count();
//                count.setId(r.getInt(1));
//                count.setNumber(r.getLong(2));
//                count.setSchoolName(r.getString(3));
//                count.setProvince(r.getString(4));
//                count.setLevel(r.getString(5));
//                count.setSchoolnature(r.getString(6));
//                count.setGuanwang(r.getString(7));
//                return count;
//            }
//        });

        spark.sparkContext().stop();
    }

    public static void txtfile(SparkContext sc,SparkSession spark){
        RDD<String> stringRDD = sc.textFile("D:\\_zhauy\\spark-testdata\\input\\user", 3);

        JavaRDD<UserInfo> us = stringRDD.toJavaRDD().map((String x) -> {
            String[] s = x.split(",");
            UserInfo u = new UserInfo();
            u.setUserId(Integer.valueOf(s[0]));
            u.setUsername(s[1]);
            u.setDate(s[2]);
            return u;
        });

        Dataset<Row> df = spark.createDataFrame(us, UserInfo.class);

        final JavaPairRDD<UserInfo, Integer> d1 = us.mapToPair(r -> new Tuple2<>(
                r, 1
        ));

    }
}
