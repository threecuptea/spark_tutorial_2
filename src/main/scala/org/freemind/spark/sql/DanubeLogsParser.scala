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

class DanubeLogsParser extends Serializable {

  //Ignore the rest of logs. We do not care about it at this case
  val nonJtLogRegEx = "\\[listener\\-\\d{1}\\] - (PUBLISH|NOPUBLISH|UNPUBLISH) (\\w+) (\\d+) \\((\\d+)\\)"

  val jtLogRegEx    = "^\\[listener\\-\\d{1}\\] - (PUBLISH|NOPUBLISH|UNPUBLISH) (\\w+)\\-(\\d+) \\((\\d+)\\)"


  val nonjtPattern:Pattern = Pattern.compile(nonJtLogRegEx)

  val jtPattern:Pattern = Pattern.compile(jtLogRegEx)


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

}