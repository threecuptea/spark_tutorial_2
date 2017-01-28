package org.freemind.spark.sql

import java.util.regex.{Matcher, Pattern}


/**
* @author sling(threecuptea) on 12/28/2016
*/
case class AccessLogRecord (
                            ipAddress: String,
                            clientIdentd: String,
                            userId: String,
                            dateTime: String,
                            method: String,
                            endPoint: String,
                            protocol: String,
                            responseCode: Int,
                            contentSize: Long
                            //referer: String,
                            //userAgent: String
                          )

class AccessLogParser extends Serializable {

  //Sample
  // 64.242.88.10 - - [07/Mar/2004:16:05:49 -0800] "GET /twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables HTTP/1.1" 401 12846
  // 64.242.88.10 - - [07/Mar/2004:16:06:51 -0800] "GET /twiki/bin/rdiff/TWiki/NewUserTemplate?rev1=1.3&rev2=1.2 HTTP/1.1" 200 4523
  val simpleAccessLogPattern = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)"

  val p:Pattern = Pattern.compile(simpleAccessLogPattern)

  def parse(s: String): Option[AccessLogRecord] = {
    val m:Matcher = p.matcher(s)
    if (m.find) {
      Some(
        AccessLogRecord(
          ipAddress = m.group(1),
          clientIdentd = m.group(2),
          userId = m.group(3),
          dateTime = m.group(4),
          method = m.group(5),
          endPoint = m.group(6),
          protocol = m.group(7),
          responseCode = m.group(8).toInt,
          contentSize = m.group(9).toLong
        )
      )
    }
    else {
      None
    }
  }



}
