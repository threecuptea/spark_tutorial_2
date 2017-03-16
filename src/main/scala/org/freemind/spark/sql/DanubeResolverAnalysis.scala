package org.freemind.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by fandev on 3/13/17.
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
    val nonJtDS = nonJtRawDS.flatMap(parser.parseNonJtResolverLog).filter($"pubId".between(lower, upper) ||
      $"pubId".between(nonJtLower, nonJtUpper)).cache()
    val jtDS = jtRawDS.flatMap(parser.parseJtResolverLog).filter($"pubId".between(lower, upper) ||
      $"pubId".between(jtLower, jtUpper)).cache()

    printf("PubId boundary for Jenga resources is [%d. %d], diff, incl.= %d.\n", lower, upper, (upper - lower ))
    printf("PubId boundary for NON-Jenga resources in NON Java-transform pubId boundary is [%d. %d], diff, inc= %d.\n",
      nonJtLower, nonJtUpper, (nonJtUpper - nonJtLower + 1))
    println(s"NON Java-transform total RESOLVED count= ${nonJtDS.count}")
    printf("PubId boundary for NON-Jenga resources in Java-transform pubId boundary is [%d. %d], diff, incl.= %d.\n", jtLower, jtUpper, (jtUpper - jtLower + 1))
    println(s"Java-transform total RESOLVED count= ${jtDS.count}")
    println()

    println()
    println("Union together to generate summary")
    val combinedDS = nonJtDS.union(jtDS)

    println("RESOLVE Count groupBy RESOURCE")
    combinedDS.groupBy($"resource").agg(sum($"jtNo"), sum($"jtYes")).withColumn("discrepancy flag",
      when($"sum(jtNo)" > 100,
      when($"sum(jtNo)" > $"sum(jtYes)", when(($"sum(jtNo)" - $"sum(jtYes)") / $"sum(jtNo)" > 0.125,
        when(($"sum(jtNo)" - $"sum(jtYes)") / $"sum(jtNo)" > 0.25, "--").otherwise("-")).otherwise(""))
        .otherwise(when(($"sum(jtYes)" - $"sum(jtNo)") / $"sum(jtNo)" > 0.125,
          when(($"sum(jtYes)" - $"sum(jtNo)") / $"sum(jtNo)" > 0.25, "++").otherwise("+")).otherwise(""))).otherwise(""))
      .sort($"resource").show(500, truncate = false)

    println("-: below 12.5%, --: below 25%; +: above 12.5%, ++: above 25% only for sum(jtNo) > 100")

  }

}
