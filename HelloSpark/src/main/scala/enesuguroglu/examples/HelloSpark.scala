package enesuguroglu.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object HelloSpark extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
    logger.info("Starting Hello Spark")
    val spark = SparkSession.builder()
      .appName("Hello Spark")
      .master("local[3]")
      .getOrCreate()

    //Process your data
    logger.info("Finished Hello Spark")
    spark.stop()
  }

}
