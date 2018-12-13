package com.wxstc.dl.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @Author: dl
  * @Date: 2018/12/12 16:03
  * @Version 1.0
  */
object SparkSqlTest {
  val conf = new SparkConf();
  val spark = SparkSession
    .builder().config(conf)
    .appName("Spark SQL basic example").master("local[*]").enableHiveSupport()
    .config("spark.some.config.option", "some-value")
//      .config("hive.exec.dynamic.partition", "true")
//      .config("hive.exec.dynamic.partition.mode", "nonstrict")
//    .config("hive.exec.max.dynamic.partitions",1000)
//    .config("hive.exec.max.dynamic.partitions.pernode",100)
//    .config("hive.exec.dynamic.partition.mode","nonstrict")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val jdbcDF11 = spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://47.94.196.195:3306")
      .option("dbtable", "bigdata.school_count")
      .option("user", "root")
      .option("password", "dl575153")
      .option("fetchsize", "10")
      .load()

    jdbcDF11.createOrReplaceTempView("school")

    //spark.catalog.cacheTable("tableName") or dataFrame.cache()
    //spark.catalog.uncacheTable("tableName") to remove the table from memory.

    //hive
//    jdbcDF11.write.format("orc").bucketBy(42, "province","schoolName").sortBy("number")
//      .mode("overwrite").saveAsTable("default.school_bucketed")

    val userSchool = spark.sql(
      """
        |SELECT school.id as schoolid,
        | test_insert_test.id as userid,
        | name,
        | number,
        | schoolName,
        | province,
        | level,
        | schoolnature,
        | guanwang
        | FROM
        |default.test_insert_test
        |left join school
        |on test_insert_test.id = school.id
      """.stripMargin)


    // Turn on flag for Hive Dynamic Partitioning  hivedongtai 分区 配置
//    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
//    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    // Create a Hive partitioned table using DataFrame API
    userSchool.write.partitionBy("province").format("hive").saveAsTable("hive_user_school")
    // Partitioned column `key` will be moved to the end of the schema.
    spark.sql("SELECT * FROM hive_user_school").show()


//    writeTestUser()

//    df.write.format("orc").mode("overwrite").saveAsTable("tt")
//    jdbcDF11.write.parquet("school.parquet")
//    jdbcDF11.show

  }

  def writeTestUser(): Unit ={
    import spark.implicits._
    val user = spark.sparkContext.parallelize(Array(
      (1,"代乐"),
      (2,"小红"),
      (3,"小军"),
      (4,"小雷"),
      (5,"嘿嘿"),
      (6,"哈哈"),
      (7,"嘻嘻"),
      (8,"啦啦"),
      (9,"呵呵"),
      (10,"灰灰")
    ))

//    val udf = user.map(x=>{
//      tmpUser(x._1,x._2)
//    }).toDF()
//
//    udf.write.format("orc").mode("overwrite").insertInto("dl.test_insert_test")
  }

  //hive  内部表与 parquet 外部表映射
  def parquetHive(userschool:DataFrame): Unit ={
    //HQL 创建hive parquet 表
    //    spark.sql(
    //      """
    //        |CREATE TABLE user_school(
    //        |schoolid int,
    //        |userid int,
    //        |name string,
    //        |number bigint,
    //        |schoolName string,
    //        |province string,
    //        |level string,
    //        |schoolnature string,
    //        |guanwang string)
    //        | STORED AS PARQUET
    //      """.stripMargin)

    userschool.write.mode(SaveMode.Overwrite).saveAsTable("user_school")


    // Prepare a Parquet data directory  将df Parquet写入hdfs  然后通过hive 外部表加载
    println("Parquet data directory =========================")
    val dataDir = "/user/L/sparkparquet/user_school"
    userschool.write.parquet(dataDir)
    // Create a Hive external Parquet table
    spark.sql(
      s"""
         |CREATE EXTERNAL TABLE user_school_parquet(
         |schoolid int,
         |userid int,
         |name string,
         |number bigint,
         |schoolName string,
         |province string,
         |level string,
         |schoolnature string,
         |guanwang string)
         |STORED AS PARQUET LOCATION '$dataDir'
       """.stripMargin).show()
    // The Hive external table should already have data
    spark.sql("SELECT * FROM user_school_parquet").show()
  }
  case class tmpUser(id:Int,name:String)
}
