package com.junrui

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object DataSetTest {
  Logger.getLogger("org.aparche.spark").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    //对于sparkSql与Hive的操作都统一成了SparkSession
    val sparkSession = SparkSession.builder().appName("sparkSession").master("local").getOrCreate()
    //
    import sparkSession.implicits._
    val ds: Dataset[Int] = Seq(1,2,10).toDS()
    //显示
//    ds.show()
    //将数据放入样例类中
    val person: Dataset[Person] = Seq(Person("huyun","22")).toDS()
//    person.show()
    //将一个json的数据放入样例类中
    val ds1: Dataset[Person] = sparkSession.read.json("d:/ssr.json").as[Person]
//    ds1.printSchema()
//    ds1.filter($"age"===4).show()
    ds1.select($"age"*10).show()
//    ds1.select("age").show()
  }

  /**
    * 样例类
    */
  case class Person(name:String,age:String)
}
