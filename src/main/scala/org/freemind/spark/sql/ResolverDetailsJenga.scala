package org.freemind.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * @author sling(threecuptea) on 3/16 -
  */
object ResolverDetailsJenga {

  def main(args: Array[String]): Unit = {

    if (args.length < 5) {
      println("Usage: ResolverDetailsJenga [non-jt-log] [jt-log] [jenga-lower] [jenga-upper] [reource-1] [resource_2]....")
      System.exit(-1)
    }

    val nonJtLog = args(0)
    val jtLog = args(1)
    val lower = args(2).toLong
    val upper = args(3).toLong
    val resources = args.slice(4, args.length)

    println(s"Request resources: ${resources.deep.mkString(" ")}")
    val spark = SparkSession
      .builder()
      .appName("ResolverDetailsJenga")
      .getOrCreate()
    import spark.implicits._

    val nonJtRawDS = spark.read.textFile(nonJtLog)
    val jtRawDS = spark.read.textFile(jtLog)

    val parser = new DanubeLogsParser(Some(resources))
    val nonJtDS = nonJtRawDS.flatMap(parser.parseResolverRaw(_)).filter($"pubId".between(lower, upper))
      .withColumnRenamed("dirty_size", "non_jt_dirty_size").cache()
    val jtDS = jtRawDS.flatMap(parser.parseResolverRaw(_, true)).filter($"pubId".between(lower, upper))
      .withColumnRenamed("dirty_size", "jt_dirty_size").cache()

    printf("PubId boundary for Jenga resources is [%d. %d], diff, incl.= %d.\n", lower, upper, (upper - lower))

    for (res <- resources) {
      println()
      printf("Resolve log entries of top 50 discrepancies for %s.\n", res)
      //Use outer join
      val joinedDS = nonJtDS.filter($"resource" === res).join(jtDS.filter($"resource" === res),
        Seq("pubId","resource","roviId", "old_pubId"), "inner").cache()
      //Find the discrepancies for those resources appear in both sort by discrepancy in descending order
      joinedDS.withColumn("diff", $"jt_dirty_size" - $"non_jt_dirty_size")
        .withColumn("difference", format_string("%,+8d", $"diff"))
          .withColumn("abs_diff", abs($"diff"))
        .sort(desc("abs_diff"))
          .select($"resource", $"roviId", $"pubId", $"old_pubId", $"non_jt_dirty_size", $"jt_dirty_size", $"difference")
          .show(50, truncate=false)

    }

  }
}