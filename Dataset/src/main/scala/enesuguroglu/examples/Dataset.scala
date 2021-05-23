package enesuguroglu.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

case class SurveyRecord(Age: Int, Gender: String, Country: String, state: String)

object Dataset extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    //Check command line data arguments
    if (args.length == 0) {
      logger.error("Usage: Dataset filename")
      System.exit(1)
    }
    //Spark Session Start Info Message
    logger.info("Starting DataSet Example")
    //Create Spark Session
    val spark = SparkSession.builder()
      .appName("Hello Dataset")
      .master("yarn")
      .getOrCreate()

    //Read csv file
    val df = spark.read
      .option("header", "true")
      .option("inferschema", "true")
      .csv(args(0))

    //Convert to dataset
    import spark.implicits._
    val ds = df.select("Age", "Gender", "Country", "state").as[SurveyRecord]

    //Type-safe filter
    val filteredds = ds.filter(row => row.Age < 40)

    //Type-safe groupby
    val countds = filteredds.groupByKey(r => r.Country).count()
    //Runtime groupby
    val countdf = filteredds.groupBy("Country").count()

    logger.info("DataFrame: " + countdf.collect().mkString(","))
    logger.info("DataSet:" + countds.collect().mkString(","))
    logger.info("Finished DataSet Spark Example")

    spark.stop()
  }

}
