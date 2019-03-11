package com.ccx

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * SQLContext的使用
  */
object SQLContextApp {
  def main(args: Array[String]){

//    val path = args(0)
    val path = "file:///Users/liuzhanlin/BigDataStudy/people.txt"

    //1）创建相应的context
    val sparkConf = new SparkConf();
    //在测试或者生产中，AppName和Master我们通常是通过脚本来指定
    sparkConf.setAppName("SQLContextApp").setMaster("local[2]");
    val sc = new SparkContext(sparkConf);

    val sqlContext = new SQLContext(sc);

    val sparkSession = SparkSession.builder().appName("SQLContextApp").getOrCreate();

    //2)相关的处理
    val people = sqlContext.read.textFile(path)
    people.printSchema()
    people.show()

    //3）关闭资源
    sc.stop()
  }
}
