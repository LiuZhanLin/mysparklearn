package com.ccx

import org.apache.spark.sql.SparkSession

object SparkStatFormatJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkStatFormatJob").master("local[2]").getOrCreate()

    val access = spark.sparkContext.textFile("file:///Users/liuzhanlin/BigDataStudy/people.txt")

    access.take(10).foreach(println)

    spark.stop()

  }

}
