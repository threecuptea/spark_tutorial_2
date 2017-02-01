package org.freemind.spark.sql

import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


/**
  * Inspired by https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/346304/2168141618055109/484361/latest.html
  * The article itself use Scala mixed with some Python using Spark 1.6 DataFrame.
  *
  * This illustrate:
  * 1. How to use org.apache.spark.sql.functions.udf (used in column) and SparkSession.udf.register (used in Spark SQL)
  * 2. Use DataFrame join
  *
  * I (sling, threecuptea) re-wrote this using Spark 2 Dataset/ DataFrame Scala only
  *
  * @author sling(threecuptea) on 12/28/2016
  */
object AnalyzeWebLog {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println("Usage: AnalyzeWebLog [access-log-file] [diamond-file]")
      System.exit(-1)
    }

    val logFile = args(0)
    val diamondFile = args(1)

    val spark = SparkSession.builder().appName("AnalyzeWebLog").getOrCreate()

    import spark.implicits._
    val rawDS = spark.read.textFile(logFile) //Dataset[String], return sql.DataFrame if spark.read.text(logFil)
    rawDS.show(10, false)

    val parser = new AccessLogParser()
    val logDS = rawDS.flatMap(parser.parse)  //Dataset[AccessLogRecord], will map filter out None in this case
    logDS.printSchema()

    spark.sql("SELECT cast(\"21/Jun/2014:10:00:00 -0700\" as timestamp)").show() //We cannot cast '21/Jun/2014:10:00:00 -0700' directly timstamp

    val parseDateUdf = udf(parseDate(_:String): Long) //used in Column
    //$"clientIdentd" is Column not TypedColumn, the returned result is DataFrame, The result of udf is a Column cast to DataType. The result is another Column
    val selectLogDF = logDS.select($"clientIdentd", $"contentSize", parseDateUdf($"dateTime").cast(TimestampType).alias("dt"),
      $"endPoint", $"ipAddress", $"responseCode")
    selectLogDF.explain()
    selectLogDF.show(10, false)

    //The function part is exactly the same as the one used in column.  The way to register is different
    logDS.createOrReplaceTempView("log_table")
    spark.udf.register("parseDate", parseDate(_:String): Long)  //Use in spark.sql, plain sql, the above udf used in column

    val logSqlDF = spark.sql(
      """
        select clientIdentd, contentSize, cast(parseDate(dateTime) as timestamp) as dt, endPoint, ipAddress, responseCode from log_table
      """
    )
    logSqlDF.explain()
    logSqlDF.show(10, false)

    //It does not matter if it is local file or distribute file in HDFS.  What matter is the format.
    /*
    Spark 1.6 does not support csv natively, You have to rely upon DataBrick's packages com.databricks:spark-csv_2.10:1.4.0,
    It works as the followings
    sqlContext.read.
      format("com.databricks.spark.csv").
      option("delimiter","\t").
      option("header","true").
      load("hdfs:///demo/data/tsvtest.tsv").show

      Spark 2.0
      spark.read.
        option("delimiter","\t").
        option("header","true").
        csv("hdfs:///demo/data/tsvtest.tsv").show
      */
    //Spark can take text, csv, json, parquet, SQL (spark.read.jdbc).  Mongodb provide adaptor already at least to 1.6
    //(Need to check if there is 2.0 adapter and also Cassandra.  I also got spark process LZ4  compression works (support gz given)
    //Check DataFrameReader scaldoc/javadoc def csv, you would see all options for csv,
    // def json, you would see all options for json
    val diamondDF = spark.read
        .option("header", true)
        .option("inferSchema", true)
        .csv(diamondFile)
    diamondDF.printSchema()
    diamondDF.show(10, false)

    //Join tthose fields for demo reason
    val combined = diamondDF.join(selectLogDF, diamondDF("price") === selectLogDF("contentSize"), "inner")

    val archiveReady = combined.select($"x", $"y", $"z", $"ipAddress", $"endPoint", $"contentSize", year($"dt").alias("year"), month($"dt").alias("month"))

    archiveReady.explain()
    archiveReady.cache() //It won't execute until after count
    archiveReady.count()

    archiveReady.explain()
    println(s"======= archive-ready count = ${archiveReady.count()}")

    archiveReady.show(10, false)

  }

  //incoming string is like "07/Mar/2004:17:21:44 -0730"
  //return seconds
  def parseDate(rawDate:String): Long = {
    // like Wed, 4 Jul 2001 12:08:56 -0700, HH is 24-hr, hh is 12-hour, Z  is RFC 822 time zone
    val parts = rawDate.split(' ')
    val sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss")
    val offset = parts(1).toInt
    //hours * 60 * 60, min * 60
    val offsetInSec = offset / 100 * 60 * 60 + (offset % 100) * 60

    sdf.parse(parts(0)).getTime / 1000 + offsetInSec
  }
}
