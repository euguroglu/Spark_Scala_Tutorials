package enesuguroglu.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession



object HelloSpark extends Serializable {

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
      .appName("Hello Spark")
      .master("local[3]")
      .getOrCreate()

    //Process your data
    logger.info("Finished Hello Spark")
    spark.stop()
  }

}
