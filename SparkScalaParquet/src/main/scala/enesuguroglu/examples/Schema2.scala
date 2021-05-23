package enesuguroglu.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

object Schema2 extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {

    //Create spark session
    val spark = SparkSession.builder()
      .appName("Spark Schema")
      .master("local[3]")
      .getOrCreate()

    //Defining schema
    val flightSchemaStruct = StructType(List(
      StructField("FL_DATE", DateType),
      StructField("OP_CARRIER", StringType),
      StructField("OP_CARRIER_FL_NUM", IntegerType),
      StructField("ORIGIN", StringType),
      StructField("ORIGIN_CITY_NAME", StringType),
      StructField("DEST", StringType),
      StructField("DEST_CITY_NAME", StringType),
      StructField("CRS_DEP_TIME", IntegerType),
      StructField("DEP_TIME", IntegerType),
      StructField("WHEELS_ON", IntegerType),
      StructField("TAXI_IN", IntegerType),
      StructField("CRS_ARR_TIME", IntegerType),
      StructField("ARR_TIME", IntegerType),
      StructField("CANCELLED", IntegerType),
      StructField("DISTANCE", IntegerType)
    ))


    //Reading csv file
    val flightTimeCsvDf = spark.read
      .format("csv")
      .option("header", "true")
      .option("path", "data/flight*.csv")
      .option("mode", "FAILFAST")
      .option("dateFormat", "M/d/y")
    //  .option("inferSchema", "true")
      .schema(flightSchemaStruct)
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
