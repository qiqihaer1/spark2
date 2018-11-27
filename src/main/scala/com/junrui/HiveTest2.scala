package com.junrui

import org.apache.spark.sql.SparkSession

object HiveTest2 {
  def main(args: Array[String]): Unit = {
    val warehouseLocation = "D:/spark-warehouse"
    val sparkSession = SparkSession.builder().appName("sparkSession").master("local").config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()
    import sparkSession.implicits._
    import sparkSession.sql
    /***
      * 1、加入spark-hive的依赖包
      * 2、如果存在derby数据库被占用的情况需要将metastore_db的元数据库进行删除
      */
//    sparkSession.sql("drop table src")
    sparkSession.sql("CREATE TABLE IF NOT EXISTS src (key Int, value STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'")
    sparkSession.sql("LOAD DATA LOCAL INPATH 'D:/data3.txt' INTO TABLE src ")
    sparkSession.sql("SELECT * FROM src").show()
    sparkSession.sql("SELECT COUNT(*) FROM src").show()
//    val sqlDF = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")
//
//    val stringsDS = sqlDF.map {
//      //模式匹配，s使后面可以引用变量名$X
//      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
//    }
//    stringsDS.show()
//

  }

}
