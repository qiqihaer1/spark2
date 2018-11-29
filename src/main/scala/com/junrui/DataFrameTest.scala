package com.junrui

import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SparkSession}

object DataFrameTest {
  def main(args: Array[String]): Unit = {
    val ss: SparkSession = SparkSession
      .builder()
      .appName("test")
      .master("local")
      .getOrCreate()
//    val df: DataFrame = ss.read.json("d:/ssr.json")
//    df.schema
//    df.show()
//   df.createOrReplaceTempView("ssr")
//    ss.sql("select age from ssr").show()
    //当文件是txt时没有表头，通过样例类在map中解决,使用了反射推断
    import ss.implicits._
    val df: DataFrame = ss.read.textFile("D:\\data.txt").map(_.split(",")).map(data=>new Order(data(0),data(1),data(2).toDouble)).toDF()
//    val df1: DataFrame = ss.read.textFile("D:\\data3.txt").map(_.split("\t")).toDF()
//    df.show()
    df.show()

  }
  case class Order(oid:String,tid:String,value:Double)
}
