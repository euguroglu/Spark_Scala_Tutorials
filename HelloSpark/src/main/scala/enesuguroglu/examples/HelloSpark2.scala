package enesuguroglu.examples

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Properties
import scala.io.Source
// This examples show defining configuration in seperate files and loading
// Using getSparkAppConf udf

object HelloSpark2 extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
    logger.info("Starting Hello Spark")
    /* Alternative to define configuration
    val sparkAppConf = new SparkConf()
    sparkAppConf.set("spark.app.name", "Hello Spark")
    sparkAppConf.set("spark.master", "Local[3]")

    val spark = SparkSession.builder()
      .config(sparkAppConf)
      .getOrCreate()
  */
    val spark = SparkSession.builder()
      .config(getSparkAppConf)
      .getOrCreate()

    logger.info("spark.conf=" + spark.conf.getAll.toString())
    //Process your data
    logger.info("Finished Hello Spark")
    spark.stop()
  }

  def getSparkAppConf: SparkConf = {
    val sparkAppConf = new SparkConf
    //set all spark configs
    val props = new Properties
    props.load(Source.fromFile("spark.conf").bufferedReader())
    props.forEach((k,v) => sparkAppConf.set(k.toString, v.toString))

    sparkAppConf
  }
}
