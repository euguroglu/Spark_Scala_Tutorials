package enesuguroglu.examples

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties
import scala.io.Source
// This examples show defining configuration in seperate files and loading
// Using getSparkAppConf udf

object _02SparkUnitTestCase extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {

    // Logger to check if data is there
    if (args.length == 0) {
      logger.error("Usage: HelloSpark filename")
      System.exit(1)
    }
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

    val surveyDf = loadSurveyDF(spark, args(0))

    // We can repartition data to simulate read by cluster in local machine test
    val partitionedsurveyDf = surveyDf.repartition(2)

    val countDf = countByCountry(partitionedsurveyDf)

    logger.info(countDf.collect().mkString("->"))
    countDf.show()

    logger.info("spark.conf=" + spark.conf.getAll.toString())
    //Process your data
    logger.info("Finished Hello Spark")
    // Below code is work around to observe sparkui because
    // That code will does not allow spark session to finish
    // Remove this code later, this is debugging only
    scala.io.StdIn.readLine()
    spark.stop()
  }

  def countByCountry(df: DataFrame): DataFrame = {
    df.where("Age < 40")
      .select("Age", "Gender", "Country", "state")
      .groupBy("Country")
      .count()
  }

  def loadSurveyDF(spark: SparkSession, dataFile: String) : DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(dataFile)
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
