package org.freemind.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by fandev on 3/14/17.
  */

case class DanubeMongoStats(resource: String, count: Long)

object DanubeMongoAnalysis {

  def parseMongoStats(line: String): DanubeMongoStats = {
    val splits = line.split(": ")
    assert(splits.length == 2)
    val splits2 = splits(0).split("\\.")
    assert(splits2.length == 2)
    DanubeMongoStats(splits2(1), splits(1).toLong)
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      println("Usage: DanubeMongoAnalysis [mongo-stats-fantv-dev] [mongo-stats-fantv-prod] [mongo-stats-rcs-prod]")
      System.exit(-1)
    }

    val fanDevPath = args(0)
    val fanProdPath = args(1)
    val rcsProdPath = args(2)

    val spark = SparkSession
      .builder()
      .appName("DanubeMongoStats")
      .getOrCreate()
    import spark.implicits._

    val fanDevDS = spark.read.textFile(fanDevPath).map(parseMongoStats).withColumnRenamed("count", "count_fantv_dev")
    val fanProdDS = spark.read.textFile(fanProdPath).map(parseMongoStats).withColumnRenamed("count", "count_fantv_prod")
    val rcsProdDS = spark.read.textFile(rcsProdPath).map(parseMongoStats).withColumnRenamed("count", "count_rcs_prod")

    val joinedDS = fanDevDS.join(fanProdDS, Seq("resource"), "left_outer").join(rcsProdDS, Seq("resource"), "left_outer")

    println("Danube Mongo stats by RESOURCE")
    joinedDS.show(500, truncate = false)

  }


}
