package org.freemind.spark.sql

import java.util.regex.{Matcher, Pattern}

/**
  * Created by fandev on 1/30/17.
  */

case class DanubeNonJTState (
                               roviId: Long,
                               resource: String,
                               nonJtState: String,
                               pubId: Long
                             )
case class DanubeJTState (
                               roviId: Long,
                               resource: String,
                               jtState: String,
                               pubId: Long
                             )

case class DanubeStates (
                           roviId: Long,
                           resource: String,
                           state: String,
                           pubId: Long,
                           jtNo: Integer,
                           jtYes: Integer
                         )

case class DanubeResolverTab (
                          resource: String,
                          roviId: Long,
                          pubId: Long,
                          state: String,
                          jtNo: Integer,
                          jtYes: Integer
                        )

case class DanubeResolverRaw (
                               resource: String,
                               roviId: Long,
                               pubId: Long,
                               old_pubId: String,
                               state: String,
                               dirty_size: Integer
                             )

class DanubeLogsParser(a: Option[Array[String]] = None) extends Serializable {

  //Ignore the rest of logs. We do not care about it at this case
  val nonJtLogRegEx = "\\[listener\\-\\d{1}\\] - (PUBLISH|NOPUBLISH|UNPUBLISH) (\\w+) (\\d+) \\((\\d+)\\)"
  val jtLogRegEx    = "\\[listener\\-\\d{1}\\] - (PUBLISH|NOPUBLISH|UNPUBLISH) (\\w+)\\-(\\d+) \\((\\d+)\\)"

  val nonjtPattern = Pattern.compile(nonJtLogRegEx)
  val jtPattern = Pattern.compile(jtLogRegEx)

  val nonJtResolverLogRegEx = "RESOLVE (\\w+) (\\d+) \\((\\d+) replacing (\\w+)\\) , (\\d+) dirty"
  val jtResolverLogRegEx    = "RESOLVE (\\w+)\\-(\\d+) \\((\\d+) replacing (\\w+)\\) , (\\d+) dirty"

  val nonjtResolverPattern = Pattern.compile(nonJtResolverLogRegEx)
  val jtResolverPattern = Pattern.compile(jtResolverLogRegEx)


  def resourcesConcat = a.get.mkString("|")

  //Use def, it will be executed only when it is called.  When it is called, it will execute def resourceConact as well.
  //It would only be called by DanubeLogsParser(Some).  I cannot put them as val otherwise they will be resolved at compile time
  //which will cause None error for DanubeLogsParser(Some) case
  def nonJtDiscResourcesPattern(): java.util.regex.Pattern = {
    val nonJtDiscResourcesRegEx = s"RESOLVE (${resourcesConcat}) (\\d+) \\((\\d+) replacing (\\w+)\\) , (\\d+) dirty"
    Pattern.compile(nonJtDiscResourcesRegEx)

  }
  def jtDiscResourcesPattern(): java.util.regex.Pattern = {
    val jtDiscResourcesRegEx    = s"RESOLVE (${resourcesConcat})\\-(\\d+) \\((\\d+) replacing (\\w+)\\) , (\\d+) dirty"
    Pattern.compile(jtDiscResourcesRegEx)

  }



  def parseNonJtLog(s: String): Option[DanubeNonJTState] = {
    val m:Matcher = nonjtPattern.matcher(s)
    if (m.find) {
      Some(
        DanubeNonJTState(
          nonJtState = m.group(1),
          resource = m.group(2),
          roviId = m.group(3).toLong,
          pubId = m.group(4).toLong
        )
      )
    }
    else {
      None
    }
  }

  def parseJtLog(s: String): Option[DanubeJTState] = {
    val m:Matcher = jtPattern.matcher(s)
    if (m.find) {
      Some(
        DanubeJTState (
          jtState = m.group(1),
          resource = m.group(2),
          roviId = m.group(3).toLong,
          pubId = m.group(4).toLong
        )
      )
    }
    else {
      None
    }
  }

  def parseNonJtLog2(s: String): Option[DanubeStates] = {
    val m:Matcher = nonjtPattern.matcher(s)
    if (m.find) {
      Some(
        DanubeStates(
          state = m.group(1),
          resource = m.group(2),
          roviId = m.group(3).toLong,
          pubId = m.group(4).toLong,
          jtNo = 1,
          jtYes = 0
        )
      )
    }
    else {
      None
    }
  }

  def parseJtLog2(s: String): Option[DanubeStates] = {
    val m:Matcher = jtPattern.matcher(s)
    if (m.find) {
      Some(
        DanubeStates (
          state = m.group(1),
          resource = m.group(2),
          roviId = m.group(3).toLong,
          pubId = m.group(4).toLong,
          jtNo = 0,
          jtYes = 1
        )
      )
    }
    else {
      None
    }
  }

  // "\\[listener\\-\\d{1}\\] - RESOLVE (\\w+) (\\d+) \\((\\d+) replacing (\\w+)\\) , (\\d+) dirty size"
  def parseNonJtResolverLog(s: String, countOnly: Boolean = false): Option[DanubeResolverTab] = {
    val m:Matcher = nonjtResolverPattern.matcher(s)
    if (m.find) {
      Some(
        DanubeResolverTab(
          state =  m.group(4) match {
            case "empty" => "new"
            case _ => "update"
          },
          resource = m.group(1),
          roviId = m.group(2).toLong,
          pubId = m.group(3).toLong,
          jtNo = if (countOnly) 1 else m.group(5).toInt,
          jtYes = 0
        )
      )
    }
    else {
      None
    }
  }

  def parseJtResolverLog(s: String, countOnly: Boolean = false): Option[DanubeResolverTab] = {
    val m:Matcher = jtResolverPattern.matcher(s)
    if (m.find) {
      Some(
        DanubeResolverTab(
          state =  m.group(4) match {
            case "empty" => "new"
            case _ => "update"
          },
          resource = m.group(1),
          roviId = m.group(2).toLong,
          pubId = m.group(3).toLong,
          jtNo = 0,
          jtYes = if (countOnly) 1 else m.group(5).toInt
        )
      )
    }
    else {
      None
    }
  }

  def parseResolverRaw(s: String, jt: Boolean = false): Option[DanubeResolverRaw] = {
    //jtDiscResourcesPattern cannot vbe al because resources: Array[String] are dynamically passed indirectly from
    // command line to form resourcesConcat, jtDiscResourcesPattern and nonJtDiscResourcesPattern are all def.
    //they won't be resolved until excution time which is what we need.
    val m:Matcher = if (jt) jtDiscResourcesPattern.matcher(s) else nonJtDiscResourcesPattern.matcher(s)
    if (m.find) {
      Some(
        DanubeResolverRaw(
          state =  m.group(4) match {
            case "empty" => "new"
            case _ => "update"
          },
          old_pubId =  m.group(4),
          resource = m.group(1),
          roviId = m.group(2).toLong,
          pubId = m.group(3).toLong,
          dirty_size = m.group(5).toInt
        )
      )
    }
    else {
      None
    }
  }


}
