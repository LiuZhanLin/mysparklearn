package com.ccx.dataFrame

import org.apache.spark.sql.SparkSession

object DataFrameFromCSVApp {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().
        appName("DataFrameFromCSVApp").
      master("local[2]").getOrCreate()



    val df = spark.read.option("header","true").option("inferSchema","true").csv("file:///user//people.csv")
    df.show()
    import spark.implicits._
    val ds = df.as[Studnt]

    ds.map(line=>line.age).show()



    spark.close()

  }

  case class Studnt(name:String,age:Int,height:Double)

}
