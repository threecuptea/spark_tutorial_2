package org.freemind.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by fandev on 3/13/17.
  */
object DanubeResolverAnalysis {

  def main(args: Array[String]): Unit = {

    if (args.length < 6) {
      println("Usage: DanubeResolverAnalysis [non-jt-log] [jt-log] [non-jt-lower] [non-jt-upper] [jt-lower] [jt-upper]")
      System.exit(-1)
    }

    val nonJtLog = args(0)
    val jtLog = args(1)
    val nonJtLower = if (args.length > 2) args(2).toLong else 0L
    val nonJtUpper = if (args.length > 2) args(3).toLong else 99999999999L
    val jtLower = if (args.length > 2) args(4).toLong else 0L
    val jtUpper = if (args.length > 2) args(5).toLong else 99999999999L

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
    //val nonJtDS = nonJtRawDS.flatMap(parser.parseNonJtLog).filter($"pubId".between(nonJtLower, nonJtUpper) && $"pubId".isin(statesInc:_*)).cache()
    val nonJtDS = nonJtRawDS.flatMap(parser.parseNonJtLog2).filter($"pubId".between(nonJtLower, nonJtUpper)).cache()
    printf("NON Java-transform pubId boundary is [%d. %d], diff, inc= %d.\n", nonJtLower, nonJtUpper, (nonJtUpper - nonJtLower + 1))
    println(s"NON Java-transform DanubeState count= ${nonJtDS.count}")
    println()

    val jtDS = jtRawDS.flatMap(parser.parseJtLog2).filter($"pubId".between(jtLower, jtUpper)).cache()
    printf("Java-transform pubId boundary is [%d. %d], diff, inc= %d.\n", jtLower, jtUpper, (jtUpper - jtLower + 1))
    println(s"Java-transform DanubeState count= ${jtDS.count}")

    println()
    println("Union together to generate summary")
    val combinedDS = nonJtDS.union(jtDS)

    println("Count groupBy RESOURCE")
    combinedDS.groupBy($"resource").agg(sum($"jtNo"), sum($"jtYes")).sort($"resource").show(500, truncate = false)

  }

}
