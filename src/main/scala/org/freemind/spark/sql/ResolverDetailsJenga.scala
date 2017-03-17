package org.freemind.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by fandev on 3/16/17.
  */
object ResolverDetailsJenga {

  def main(args: Array[String]): Unit = {

    if (args.length < 4) {
      println("Usage: ResolverDetailsJenga [non-jt-log] [jt-log] [jenga-lower] [jenga-upper]")
      System.exit(-1)
    }

    val nonJtLog = args(0)
    val jtLog = args(1)
    val lower = args(2).toLong
    val upper = args(3).toLong

    val spark = SparkSession
      .builder()
      .appName("ResolverDetailsJenga")
      .getOrCreate()
    import spark.implicits._

    val nonJtRawDS = spark.read.textFile(nonJtLog)
    val jtRawDS = spark.read.textFile(jtLog)

    val parser = new DanubeLogsParser()
    val nonJtDS = nonJtRawDS.flatMap(parser.parseResolverRaw(_)).filter($"pubId".between(lower, upper))
      .withColumnRenamed("dirty_size", "non_jt_dirty_size").cache()
    val jtDS = jtRawDS.flatMap(parser.parseResolverRaw(_, true)).filter($"pubId".between(lower, upper))
      .withColumnRenamed("dirty_size", "jt_dirty_size").cache()

    printf("PubId boundary for Jenga resources is [%d. %d], diff, incl.= %d.\n", lower, upper, (upper - lower))

    for (res <- Seq("NameInfoAka", "VideoCreativeWorkiGuide", "VideoWorkDescription" )) {
      println()
      printf("Resolve log entries of top 25 discrepancies for %s.\n", res)
      val joinedDS = nonJtDS.filter($"resource" === res).join(jtDS.filter($"resource" === res),
        Seq("pubId","resource","roviId", "old_pubId"), "inner").cache()
      joinedDS.withColumn("abs_diff", abs($"non_jt_dirty_size" - $"jt_dirty_size")).sort(desc("abs_diff")).show(25, truncate=false)

    }








  }
}