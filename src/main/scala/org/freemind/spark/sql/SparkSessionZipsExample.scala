package org.freemind.spark.sql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._


/**
  * Inspied by http://cdn2.hubspot.net/hubfs/438089/notebooks/spark2.0/SparkSessionZipsExample.html
  * It use Spark2 Dataset/ DataFrame and illustrate basic DataFrame usage
  *
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

    import spark.implicits._

    val lst = List(("Scala", 35), ("Python", 30), ("R", 15), ("Java", 20))
    val lstDF = spark.createDataFrame(lst).toDF("language", "percent")
    //orderBy is Dataset function
    lstDF.orderBy(desc("percent")).show()

    //.as[Zips] make this a Dataset that I can use typed operation like retrieve field
    val zipDS = spark.read.json(jsonFile).withColumnRenamed("_id", "zip").as[Zips]
    zipDS.printSchema()

    println("Population > 40000")

    //I can use DataSet filter method as the first line.  DataSet is a typed object. Therefore, I can retrieve 'pop' field.
    // In Spark 1.6, I can only use DataFrame filter method as the second line.  DataFrame is not typed object in Spark 1.6
    zipDS.filter(_.pop > 40000).select($"state", $"city", $"zip", $"pop").orderBy(desc("pop")).show(10, false)
    //zipDS.select($"state", $"city", $"zip", $"pop").filter($"pop" > 40000).orderBy(desc("pop")).show(10, false)

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
    spark.catalog.cacheTable("hive_zips_table") //cache the table in memory

    println("zipToInt UDF")
    //The udf is used in sql
    spark.udf.register("zipToInt", (z: String) => z.toInt)
    spark.sql("SELECT city, zipToInt(zip) AS zipInt FROM hive_zips_table ORDER BY zipInt").show(10, false)

    spark.catalog.listTables().show()
  }

}

