package com.ccx.dataFrame

import org.apache.spark.sql.SparkSession

/**
  * DataFrame 和RDD 的互操作
  */
object DataFrameRDDApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[2]").appName("DataFrameRDDApp").getOrCreate()

    //获取rdd
    val rdd = spark.sparkContext.textFile("file:///Users/liuzhanlin/BigDataStudy/people.txt")

    //注意：需要导入隐式转换
    import spark.implicits._
    val df = rdd.map(_.split(",")).map(line => Info(line(0),line(1).toInt,line(2).toDouble)).toDF()

    df.printSchema()
    df.createTempView("people")

    df.sqlContext.sql("select * from people").show()

    spark.close()

  }

  case class Info(name:String,age:Int,height:Double)
}
