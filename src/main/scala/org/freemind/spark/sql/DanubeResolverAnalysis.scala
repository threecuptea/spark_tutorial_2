package org.freemind.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * DanubeResolverAnalysis generate a similar crosstab report like DanubeStateAnalysis2 but based upon resolver log
  * instead of transformer.log.
  *
  * It adds "difference" display field by using format_string and add "diff. flag" field by using
  * nested when and otherwise sql functions on numeric conditions
  *
  * @author sling(threecuptea) on 3/13 -
  */
object DanubeResolverAnalysis {

  def main(args: Array[String]): Unit = {

    if (args.length < 8) {
      println("Usage: DanubeResolverAnalysis [non-jt-log] [jt-log] [jenga-lower] [jenga-upper] [non-jt-lower] [non-jt-upper] [jt-lower] [jt-upper]")
      System.exit(-1)
    }

    val nonJtLog = args(0)
    val jtLog = args(1)
    val lower = args(2).toLong
    val upper = args(3).toLong
    val nonJtLower = args(4).toLong
    val nonJtUpper = args(5).toLong
    val jtLower = args(6).toLong
    val jtUpper = args(7).toLong
    val countOnly = if (args.length > 8) args(8).toBoolean else false
    
    val spark = SparkSession
      .builder()
      .appName("DanubeResolverAnalysis")
      .getOrCreate()
    import spark.implicits._

    val nonJtRawDS = spark.read.textFile(nonJtLog)
    val jtRawDS = spark.read.textFile(jtLog)

    val parser = new DanubeLogsParser()
    //Do the followings if I only want to include PUBLISH and UNPUBLISH state in the report
    //val statesInc = Seq("PUBLISH", "UNPUBLISH") //_* expanded to var args
    //val nonJtDS = nonJtRawDS.flatMap(parser.parseNonJtLog).filter($"pubId".between(nonJtLower, nonJtUpper) && $"state".isin(statesInc:_*)).cache()
    val nonJtDS = nonJtRawDS.flatMap(parser.parseNonJtResolverLog(_, countOnly)).filter($"pubId".between(lower, upper) ||
      $"pubId".between(nonJtLower, nonJtUpper)).cache()
    val jtDS = jtRawDS.flatMap(parser.parseJtResolverLog(_, countOnly)).filter($"pubId".between(lower, upper) ||
      $"pubId".between(jtLower, jtUpper)).cache()

    printf("PubId boundary for Jenga resources is [%d. %d], diff, incl.= %d.\n", lower, upper, (upper - lower ))
    printf("PubId boundary for NON-Jenga resources in NON Java-transform pubId boundary is [%d. %d], diff, inc= %d.\n",
      nonJtLower, nonJtUpper, (nonJtUpper - nonJtLower + 1))
    println(s"NON Java-transform total RESOLVED dirty count= ${nonJtDS.count}")
    printf("PubId boundary for NON-Jenga resources in Java-transform pubId boundary is [%d. %d], diff, incl.= %d.\n", jtLower, jtUpper, (jtUpper - jtLower + 1))
    println(s"Java-transform total RESOLVED dirty count= ${jtDS.count}")
    println()

    println()
    println("Union together to generate summary")
    val combinedDS = nonJtDS.union(jtDS)
    if (countOnly) {
      println("RESOLVE line count groupBy RESOURCE")
    } else {
      println("RESOLVE dirty size groupBy RESOURCE")
    }
    combinedDS.groupBy($"resource").agg(sum($"jtNo"), sum($"jtYes"))
      .withColumn("diff", $"sum(jtYes)"- $"sum(jtNo)")
      .withColumn("difference", format_string("%,+8d", $"diff"))
      .withColumn("flag",
      when($"sum(jtNo)" > 1000,
      when($"diff" < 0,
        when(abs($"diff") / $"sum(jtNo)" > 0.125,
        when(abs($"diff") / $"sum(jtNo)" > 0.25, "==")
        .otherwise("="))
        .otherwise(""))
      .otherwise(when($"diff" / $"sum(jtNo)" > 0.125,
        when($"diff" / $"sum(jtNo)" > 0.25, "++")
          .otherwise("+"))
        .otherwise("")))
          .otherwise(""))
      .sort($"resource")
        .select($"resource", $"sum(jtNo)", $"sum(jtYes)", $"difference", $"flag")
        .show(500, truncate = false)

    println("=: below 12.5%, ==: below 25%; +: above 12.5%, ++: above 25% only for sum(jtNo) > 1000")

  }

}
