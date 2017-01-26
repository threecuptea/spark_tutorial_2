package org.freemind.spark.sql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._


/*
org.apache.spark.sql.functions._ is an object that encapsulate all sql functions, including aggregate, collection(explode):
date_time, sorting(asc, desc), math, non-aggregate(col, expr, lit), string(lower, split, length etc.) and udf
date_time: year, month, current_date, current_timestamp
 */

/**
  * Inspied by http://cdn2.hubspot.net/hubfs/438089/notebooks/spark2.0/SparkSessionZipsExample.html
  *
  * I first write on 9/2/2016
  * @author sling(threecuptea) on 12/28/2016
  */
case class Zips(zip: String, city: String, loc:Array[Double], pop: Long, state: String)

object SparkSessionZipsExample {

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      println("Usage: SparkSessionZipsExample [json-file]")
      System.exit(-1)
    }

    val jsonFile = args(0)
    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"
    val spark = SparkSession.builder()
      .appName("SparkSessionZipsExample")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    //The following line would include all Encoders which are needed to convert any DS, DF
    import spark.implicits._

    val lst = List(("Scala", 35), ("Python", 30), ("R", 15), ("Java", 20))
    val lstDF = spark.createDataFrame(lst).toDF("language", "percent")
    //orderBy is Dataset function
    lstDF.orderBy(desc("percent")).show()

    //It does not matter if it is local file or distribute file in HDFS.
    val zipDS = spark.read.json(jsonFile).withColumnRenamed("_id", "zip").as[Zips]
    zipDS.printSchema()

    println("Population > 40000")
    //select Column (not TypedColumn), filter(condition: Column) peopleDs.filter($"age" > 15)
    zipDS.select($"state", $"city", $"zip", $"pop").filter($"pop" > 40000).orderBy(desc("pop")).show(10, false)

    println("States in order of population")
    //sum("columnName") or sum(Col): Column
    zipDS.select($"state", $"pop").groupBy($"state").sum("pop").orderBy(desc("sum(pop)")).show(10, false)

    println("California cities in order of population, count zip and sum pop")
    zipDS.filter($"state" === "CA").groupBy($"city").agg(count($"zip"), sum($"pop"))
      .withColumnRenamed("count(zip)", "num_zips").withColumnRenamed("sum(pop)", "population")
      .orderBy(desc("population")).show(10, false)

    zipDS.createOrReplaceTempView("zips_table")
    spark.sql("select city, COUNT(zip) AS num_zips, SUM(pop) AS population FROM zips_table WHERE state = 'CA' GROUP BY city ORDER BY SUM(pop) DESC").show(10, false)

    /**
      * save as Hive table
      */
    spark.sql("DROP TABLE IF EXISTS hive_zips_table")
    zipDS.write.saveAsTable("hive_zips_table")
    spark.catalog.cacheTable("hive_zips_table") //cache teh table in memory

    println("zipToInt UDF")
    //The udf is used in sql
    spark.udf.register("zipToInt", (z: String) => z.toInt)
    spark.sql("SELECT city, zipToInt(zip) AS zipInt FROM hive_zips_table ORDER BY zipInt").show(10, false)

    spark.catalog.listTables().show()
  }

}

