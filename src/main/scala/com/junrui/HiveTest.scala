package com.junrui

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object HiveTest {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    //获取SparkSession,启动hive的支持
    val warehouseLocation = "D:/spark-warehouse"
    val sparkSession = SparkSession.builder().appName("sparkSession").master("local").config("spark.sql.warehouse.dir",warehouseLocation).
    enableHiveSupport().getOrCreate()
    import sparkSession.implicits._
    import sparkSession.sql
    /***
      * 1、加入spark-hive的依赖包
      * 2、如果存在derby数据库被占用的情况需要将metastore_db的元数据库进行删除
      */
    sparkSession.sql("drop table dota")
    sparkSession.sql("CREATE TABLE IF NOT EXISTS dota (key STRING, value STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'")
    sparkSession.sql("LOAD DATA LOCAL INPATH 'd:/service.txt' INTO TABLE dota")
    sparkSession.sql("select * from dota").show()
    sparkSession.sql("select count(*) from dota").show()

  }

}
