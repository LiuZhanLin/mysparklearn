package com.ccx.dataFrame

import org.apache.spark.sql.SparkSession

/**
  * DataFrame 常见的API
  */
object DataFrameApp {

  def main(args: Array[String]): Unit = {

    //得到sparkSession
    val spark = SparkSession.builder().appName("DataFrameApp").master("local[2]").getOrCreate()

    //得到dataFrame
    val peopleDF = spark.read.format("json").load("file:///Users/liuzhanlin/BigDataStudy/json.json")


    //打印schema信息
    peopleDF.printSchema()

    //输出前面的20条信息
//    peopleDF.show(）

    //类似于查询指定的列，方法1
    peopleDF.select("age").show()

    //查询指定列方法2
    peopleDF.select(peopleDF.col("name"),(peopleDF.col("age")+10).as("age2")).show()

    //根据条件过滤
    peopleDF.filter(peopleDF.col("age")>30).show()

    //根据指定的字段进行分组
    peopleDF.groupBy("deptno").count().show()


    //关闭spark
    spark.stop()





  }
}
