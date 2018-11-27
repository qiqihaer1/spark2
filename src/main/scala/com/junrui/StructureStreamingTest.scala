package com.junrui

import org.apache.spark.sql.SparkSession

object StructureStreamingTest {
  def main(args: Array[String]): Unit = {
    //获取sqlcontext
    val sparkSession = SparkSession.builder().appName("StructuredNetworkWordCount").getOrCreate()
    val lines = sparkSession.readStream.format("socket").option("host", "192.168.200.21").option("port", 9999).load()
    // Split the lines into words
    import sparkSession.implicits._
    val words = lines.as[String].flatMap(_.split("  "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()


    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream.outputMode("complete").format("console").start()

    query.awaitTermination()

  }

}
