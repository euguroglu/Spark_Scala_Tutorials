package enesuguroglu.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession


object Schema extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {

    //Create spark session
    val spark = SparkSession.builder()
      .appName("Spark Schema")
      .master("local[3]")
      .getOrCreate()

    //Reading csv file
    val flightTimeCsvDf = spark.read
      .format("csv")
      .option("header", "true")
      .option("path", "data/flight*.csv")
      .option("inferSchema", "true")
      .load()

    flightTimeCsvDf.show(5)
    logger.info("CSV Schema:" + flightTimeCsvDf.schema.simpleString)

    //Reading json file
    val flightTimeJsonDf = spark.read
      .format("json")
      .option("path", "data/flight*.json")
      .load()

    flightTimeJsonDf.show(5)
    logger.info("Json Schema:" + flightTimeJsonDf.schema.simpleString)

    val flightTimeParquetDf = spark.read
      .format("parquet")
      .option("path", "data/flight*.parquet")
      .load()

    flightTimeParquetDf.show(5)
    logger.info("Parquet Schema:" + flightTimeParquetDf.schema.simpleString)

    spark.stop()
  }
}
