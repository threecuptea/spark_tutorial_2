package org.freemind.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by fandev on 7/3/17.
  */
case class FlightExcerpt(quarter: Int, origin: String, dest: String, depdelay: Int, cancelled: Int)
object FlightSample {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: FlightSample [input-path] [s3-putput-path] ")
      System.exit(-1)
    }

    val inPath = args(0)
    //val outPath = args(1)

    val spark = SparkSession
      .builder()
      .appName("FlightSample")
      .getOrCreate()
    import spark.implicits._

    val rawDF = spark.read.parquet(inPath)
    rawDF.printSchema()
    rawDF.show(5)

    val flightDS = rawDF.filter($"year" >= 1990).select($"quarter", $"origin", $"dest", $"depdelay", $"cancelled").cache()

    /** top departure by origin */
    flightDS.groupBy($"origin").count().withColumnRenamed("count", "total_departures")
      .sort(desc("total_departures")).limit(10).show()

    /** top short delay by origin */
    flightDS.filter($"depdelay" >= 15).groupBy($"origin").count().withColumnRenamed("count", "total_delays(>=15)")
      .sort(desc("total_delays(>=15)")).limit(10).show()

    /** top long delay by origin */
    flightDS.filter($"depdelay" >= 60).groupBy($"origin").count().withColumnRenamed("count", "total_delays(>=60)")
      .sort(desc("total_delays(>=60)")).limit(10).show()

    /** top cancellation by origin */
    flightDS.filter($"cancelled" === 1).groupBy($"origin").count().withColumnRenamed("count", "total_cancellations")
      .sort(desc("total_cancellations")).limit(10).show()

    /** top cancellation by quarter */
    flightDS.filter($"cancelled" === 1).groupBy($"quarter").count().withColumnRenamed("count", "total_cancellations")
      .sort(desc("total_cancellations")).limit(10).show()

    flightDS.groupBy($"origin", $"dest").count().withColumnRenamed("count", "total_flights")
      .sort(desc("total_flights")).limit(10).show()

  }

}
