package com.ccx.dataFrame

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession, types}

/**
  * DataFrame 和RDD 的互操作
  */
object DataFrameRDDApp2 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[2]").appName("DataFrameRDDApp").getOrCreate()

    //获取rdd
    val rdd = spark.sparkContext.textFile("file:///Users/liuzhanlin/BigDataStudy/people.txt")

    //注意：需要导入隐式转换
    import spark.implicits._
    val infoRDD = rdd.map(_.split(",")).map(line => Row(line(0),line(1).toInt,line(2).toDouble))

    val structType = StructType(Array(StructField("namIntTypee",StringType,true),
      StructField("age",IntegerType,true),
      StructField("height",DoubleType,true)))

    val dataFrame = spark.createDataFrame(infoRDD,structType)

    dataFrame.show()
    spark.close()

  }

}
