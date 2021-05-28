package enesuguroglu.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class ApacheLogRecord(ip: String, date: String, request: String, referrer: String)

object LogFile extends Serializable {
  @transient lazy val logger : Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Log File")
      .master("local[3]")
      .getOrCreate()

    val df = spark.read.textFile("data/apache_logs.txt").toDF()

    val myReg = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)"""".r

    import spark.implicits._
    val df_log = df.map(row =>
      row.getString(0) match {
        case myReg (ip, client, user, date, cmd, request, proto, status, bytes, referrer, userAgent) =>
          ApacheLogRecord(ip, date, request, referrer)
      }
    )
    val df_final = df_log.withColumn("referrer", substring_index($"referrer", "/", 3))
    df_final.groupBy("referrer").count().show(truncate=false)
    spark.stop()
  }

}
