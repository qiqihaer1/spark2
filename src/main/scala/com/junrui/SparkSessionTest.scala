package com.junrui


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}


object SparkSessionTest {
  //简化控制台
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    //获取SparkSession
    val sparkSession = SparkSession.builder().appName("spark2").master("local").getOrCreate()

    //读取json文件获得DF
    val df: DataFrame = sparkSession.read.json("d://ssr.json")
    df.printSchema()
    //获得临时视图hello
    df.createOrReplaceTempView("hello")
    //查询
    val frame: DataFrame = sparkSession.sql("select age from hello")
    //显示
    frame.show()

  }

}
