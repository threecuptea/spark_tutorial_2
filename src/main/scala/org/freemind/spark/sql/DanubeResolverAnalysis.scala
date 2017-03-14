package org.freemind.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by fandev on 3/13/17.
  */
object DanubeResolverAnalysis {

  def main(args: Array[String]): Unit = {

    if (args.length < 4) {
      println("Usage: DanubeResolverAnalysis [non-jt-log] [jt-log] [lower] [upper]")
      System.exit(-1)
    }

    val nonJtLog = args(0)
    val jtLog = args(1)
    val lower = if (args.length > 2) args(2).toLong else 0L
    val upper = if (args.length > 2) args(3).toLong else 99999999999L

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
    val nonJtDS = nonJtRawDS.flatMap(parser.parseNonJtResolverLog).filter($"pubId".between(lower, upper)).cache()

    val jtDS = jtRawDS.flatMap(parser.parseJtResolverLog).filter($"pubId".between(lower, upper)).cache()

    printf("pubId boundary for Jenga resources is [%d. %d], diff, inc= %d.\n", lower, upper, (upper - lower ))
    println()

    println()
    println("Union together to generate summary")
    val combinedDS = nonJtDS.union(jtDS)

    println("Count groupBy RESOURCE")
    combinedDS.groupBy($"resource").agg(sum($"jtNo"), sum($"jtYes")).sort($"resource").show(500, truncate = false)

  }

}
