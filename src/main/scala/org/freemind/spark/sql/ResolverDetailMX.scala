package org.freemind.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * @author sling/threecuptea on 3/25/17.
  */
object ResolverDetailMX {

  def main(args: Array[String]): Unit = {

    if (args.length < 5) {
      println("Usage: ResolverDetailsJenga [non-jt-log] [jt-log] [jenga-lower] [jenga-upper] [state] [reource-1]....")
      System.exit(-1)
    }
//908288029
    val nonJtLog = args(0)
    val jtLog = args(1)
    val lower = args(2).toLong
    val upper = if (args(3).toLong == -1L) 99999999999L else args(3).toLong
    val state = args(4)
    val resources = args.slice(5, args.length) //Take the rest args for interested resources.

    println(s"Request resources: ${resources.deep.mkString(" ")}")
    val spark = SparkSession
      .builder()
      .appName("ResolverDetailsMX")
      .getOrCreate()
    import spark.implicits._

    val nonJtRawDS = spark.read.textFile(nonJtLog)
    val jtRawDS = spark.read.textFile(jtLog)

    val parser = new DanubeLogsParser(Some(resources))
    val nonJtDS = nonJtRawDS.flatMap(parser.parseResolverRaw(_)).filter($"roviId".between(lower, upper) && $"state" === state)
      .withColumnRenamed("dirty_size", "non_jt_dirty_size").withColumnRenamed("pubId", "non_jt_pubId").cache()
    val jtDS = jtRawDS.flatMap(parser.parseResolverRaw(_, true)).filter($"roviId".between(lower, upper) && $"state" === state)
      .withColumnRenamed("dirty_size", "jt_dirty_size").withColumnRenamed("pubId", "jt_pubId").cache()

    printf("PubId boundary for Jenga resources is [%d. %d], diff, incl.= %d.\n", lower, upper, (upper - lower))

    for (res <- resources) {
      println()
      printf("Resolve log entries of top 500 discrepancies for %s.\n", res)
      //Use outer join
      val joinedDS = nonJtDS.filter($"resource" === res).join(jtDS.filter($"resource" === res),
        Seq("resource","roviId", "state"), "outer").cache()
      //Find those resourceIds do not appear in one or the other.  Try to find those MX resource that look back in
      // one environment but not in another environment. Emphasize on missing Jt one,
      // jt_dirty_size.isNull flag 0 will be sorted on the top.
      joinedDS.filter($"non_jt_dirty_size".isNull || $"jt_dirty_size".isNull)
        .withColumn("flag", when($"jt_dirty_size".isNull, 0).otherwise(1))
        .sort($"flag", $"non_jt_pubId")
        .select($"resource", $"roviId", $"non_jt_pubId", $"non_jt_dirty_size", $"jt_pubId", $"jt_dirty_size")
        .show(500, truncate=false)

    }

  }

}
