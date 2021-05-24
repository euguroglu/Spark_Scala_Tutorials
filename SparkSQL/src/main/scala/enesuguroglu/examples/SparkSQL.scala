package enesuguroglu.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkSQL extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark SQL Table")
      .master("local[3]")
      .enableHiveSupport()
      .getOrCreate()

    logger.info("Reading from dataSource")
    val df = spark.read
      .format("parquet")
      .option("path", "dataSource/")
      .load()

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    df.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      //.partitionBy("ORIGIN", "OP_CARRIER")
      .bucketBy(5, "ORIGIN", "OP_CARRIER")
      .sortBy("ORIGIN", "OP_CARRIER")
      .saveAsTable("flight_data_tlb")

    spark.catalog.listTables("AIRLINE_DB").show()
    logger.info("Finished")
    spark.stop()
  }

}
