package org.freemind.spark.sql

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, udf}

/**
  * This is for station_overlay
  *
  * @author sling/ threecuptea created on 5/25/2018
  */
object StationOverlaysProcessing {

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      println("Usage: StationOverlaysProcessing [overlay-path]")
      System.exit(-1)
    }

    val overlayPath = args(0)

    val spark = SparkSession.builder().appName("station-overlays").getOrCreate()
    import spark.implicits._

    val csv = spark.read.option("header", true)
      .option("inferSchema", true).csv(overlayPath)

    println(s"total= ${csv.count()}")
    csv.printSchema()

    val wrapper = new IdGenWrapper
    //Create a user defined function in this way
    val genUdf = udf(wrapper.computeGen(_: String, _: Int): Long)

    val overlays = csv.withColumn("resource_type", lit("station_overlay")).
      withColumn("st_numeric", genUdf(lit("st"), $"mfsid"))

    val overlaysConfig = WriteConfig(Map("uri" -> "mongodb://localhost:27017/unified.OverlaysLookup"))

    val start = System.currentTimeMillis()
    MongoSpark.save(overlays.select($"sourceStationId".as("rovi_id"), $"resource_type",
      $"mfsid", $"st_numeric").write.mode("append"), overlaysConfig)

    printf("Execution time= %7.3f seconds\n", (System.currentTimeMillis() - start) / 1000.00)

  }
}
