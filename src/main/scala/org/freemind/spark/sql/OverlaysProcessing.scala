package org.freemind.spark.sql

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import com.tivo.unified.IdGen.gen
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * This parses Tivo's show content/ collection specification CSV file
  * 1. To categorize using SHOWTYPE etc. criteria
  * 2. Call java static method (IdGen class) & get sbt java/ scala build working and put together a udf type to
  * generate overlay numeric value for content/ collection ids.
  * 3. Save key values to Mongodb  OverlaysLookup collection of unified database so that I can create IdAuthResponse simulator.
  *
  * @author sling/ threecuptea 04/21/2018
  */
object OverlaysProcessing {

  //Wrap a java function and specify compileOrder := CompileOrder.JavaThenScala in build.sbt
  def computeGen(isColl: Boolean, key: Int): Long = gen(isColl, key)

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      println("Usage: OverlaysProcessing [overlay-path]")
      System.exit(-1)
    }

    val overlayPath = args(0)

    val spark = SparkSession.builder().appName("overlays-processing").getOrCreate()
    import spark.implicits._

    val csv = spark.read.option("header", true)
      .option("inferSchema", true).csv(overlayPath)

    println(s"total= ${csv.count()}")
    csv.printSchema()

    //Create a user defined function in this way
    val genUdf = udf(computeGen(_: Boolean, _: Int): Long)

    val overlays = csv.withColumn("resource_type", when($"SHOWTYPE" === 8, "movie_overlay").
                                          when($"SHOWTYPE" === 3, "other_overlay").
                                          when($"SHOWTYPE" === 5, when($"TMSID".startsWith("EP"), "episode_overlay").
                                          otherwise("series_overlay"))).
      withColumn("coll_tmsid", $"SERIESTMSID".substr(3, 10).cast("int")).
      withColumn("ct_numeric", genUdf(lit(false), $"mfsid")).
      withColumn("cl_numeric", genUdf(lit(true), $"coll_tmsid"))

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