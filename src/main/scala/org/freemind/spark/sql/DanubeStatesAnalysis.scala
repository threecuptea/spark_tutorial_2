package org.freemind.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by fandev on 1/30/17.
  */
object DanubeStatesAnalysis {

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      println("Usage: AnalyzeDanubeStates [NON-java-transform-log] [java-transform-log] [output-dir]")
      System.exit(-1)
    }

    val nonJtLog = args(0)
    val jtLog = args(1)
    val output = args(2)

    val spark = SparkSession
      .builder()
      .appName("MovieLensALS")
      .getOrCreate()
    import spark.implicits._

    val nonJtRawDS = spark.read.textFile(nonJtLog)
    val jtRawDS = spark.read.textFile(jtLog)

    val parser = new DanubeLogsParser()
    val nonJtDS = nonJtRawDS.flatMap(parser.parseNonJtLog)
    val jtDS = jtRawDS.flatMap(parser.parseJtLog)

    println(s"NON Java-transform DanubeState snapshot= ${nonJtDS.count}")
    nonJtDS.show(10)
    println(s"Java-transform DanubeState snapshot= ${jtDS.count}")
    jtDS.show(10)

    val sharedDS = nonJtDS.join(jtDS, Seq("roviId", "resource"), "inner")
    println(s"Shared ResourceId= ${sharedDS.count}")
    sharedDS.show(10)

    val discDS = sharedDS.filter("nonJtState != jtState")
    println(s"Discrepancy ResourceId= ${discDS.count}")
    discDS.show(10)

    val percent =  discDS.count / sharedDS.count.toFloat * 100
    printf("Discrepancy percent is %3.2f%%.\n", percent)
    discDS.write.option("header", true).csv(output)

  }

}
