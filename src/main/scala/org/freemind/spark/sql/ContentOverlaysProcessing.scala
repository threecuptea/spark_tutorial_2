package org.freemind.spark.sql

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, udf, when}

/**
  * This parses Tivo's show content/ collection specification CSV file
  * 1. To categorize using SHOWTYPE etc. criteria
  * 2. Call java static method (IdGen class) & get sbt java/ scala build working and put together a udf type to
  * generate overlay numeric value for content/ collection ids.
  * 3. Save key values to Mongodb  OverlaysLookup collection of unified database so that I can create IdAuthResponse simulator.
  *
  * $SPARK_HOME/bin/spark-submit --master local[4] --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.2 \
  * --class org.freemind.spark.sql.ContentOverlaysProcessing target/scala-2.11/spark_tutorial_2_2.11-1.0.jar data/content_overlays
  *
  * @author sling/ threecuptea 04/21/2018, modified on 5/25/2018
  */

object ContentOverlaysProcessing {

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      println("Usage: ContentOverlaysProcessing [overlay-path]")
      System.exit(-1)
    }

    val overlayPath = args(0)

    val spark = SparkSession.builder().appName("content-overlays").getOrCreate()
    import spark.implicits._

    val csv = spark.read.option("header", true)
      .option("inferSchema", true).csv(overlayPath)

    println(s"total= ${csv.count()}")
    csv.printSchema()

    val wrapper = new IdGenWrapper
    //Create a user defined function in this way
    val genUdf = udf(wrapper.computeGen(_: String, _: Int): Long)

    val overlays = csv.withColumn("resource_type", when($"SHOWTYPE" === 8, "movie_overlay").
      when($"SHOWTYPE" === 3, "other_overlay").
      when($"SHOWTYPE" === 5, when($"TMSID".startsWith("EP"), "episode_overlay").
        otherwise("series_overlay"))).
      withColumn("coll_tmsid", $"SERIESTMSID".substr(3, 10).cast("int")).
      withColumn("ct_numeric", genUdf(lit("ct"), $"mfsid")).
      withColumn("cl_numeric", genUdf(lit("cl"), $"coll_tmsid"))

    overlays.groupBy("resource_type").count().show(false)
    println()

    println("Movie samples:")
    overlays.filter($"resource_type" === "movie").
      select($"sourceProgramId".as("rovi_id"), $"resource_type",
        $"mfsid", $"ct_numeric", $"coll_tmsid", $"cl_numeric", $"TITLE".as("title")).show(10, false)

    val overlaysConfig = WriteConfig(Map("uri" -> "mongodb://localhost:27017/unified.OverlaysLookup"))

    val start = System.currentTimeMillis()
    MongoSpark.save(overlays.select($"sourceProgramId".as("rovi_id"), $"resource_type",
      $"mfsid", $"ct_numeric", $"coll_tmsid", $"cl_numeric", $"TITLE".as("title")).write.mode("overwrite"), overlaysConfig)

    printf("Execution time= %7.3f seconds\n", (System.currentTimeMillis() - start)/1000.00)

  }

}
