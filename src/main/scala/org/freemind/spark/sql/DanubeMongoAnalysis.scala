package org.freemind.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by fandev on 3/14/17.
  */

case class DanubeMongoStats(resource: String, count: Long)

object DanubeMongoAnalysis {

  def parseMongoStats(line: String): DanubeMongoStats = {
    val splits = line.split(" - ")
    assert(splits.length == 2)
    val splits2 = splits(1).split(" ")
    assert(splits2.length == 2)
    DanubeMongoStats(splits(0), splits2(0).toLong)
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println("Usage: DanubeMongoAnalysis [mongo-stats-fantv-prod] [mongo-stats-fantv-dev]")
      System.exit(-1)
    }

    val fanProdPath = args(0)
    val fanDevPath = args(1)


    val spark = SparkSession
      .builder()
      .appName("DanubeMongoStats")
      .getOrCreate()
    import spark.implicits._

    val fanProdDS = spark.read.textFile(fanProdPath).map(parseMongoStats).withColumnRenamed("count", "count_fantv_prod").cache()
    val fanDevDS = spark.read.textFile(fanDevPath).map(parseMongoStats).withColumnRenamed("count", "count_fantv_dev").cache()

    val countProd = fanProdDS.filter(!$"resource".startsWith("MX") && !$"resource".startsWith("Published")).groupBy().sum("count_fantv_prod").first().getLong(0)
    val countDev = fanDevDS.filter(!$"resource".startsWith("MX") && !$"resource".startsWith("Published")).groupBy().sum("count_fantv_dev").first().getLong(0)

    println("===============================================================================================================")
    println(s"Total Jenga resource count in PROD vs DEV and (DEV - PROD): ${countProd}, ${countDev}, ${countDev - countProd}")
    println("===============================================================================================================")
    println()

    val joinedDS = fanProdDS.join(fanDevDS, Seq("resource"), "right_outer")
              .withColumn("diff", when(isnull($"count_fantv_prod"), $"count_fantv_dev").otherwise($"count_fantv_dev" - $"count_fantv_prod") )
              .withColumn("difference", format_string("%,+8d", $"diff")).sort("resource")

    println("Danube Mongo stats(count) by RESOURCE")
    joinedDS.select($"resource", $"count_fantv_prod", $"count_fantv_dev", $"difference").show(500, truncate = false)

  }


}
