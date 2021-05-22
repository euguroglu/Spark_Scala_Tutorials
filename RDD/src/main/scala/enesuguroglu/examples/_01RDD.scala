package enesuguroglu.examples

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

case class SurveyRecord(Age: Int, Gender: String, Country: String, state: String)

object _01RDD extends Serializable {
  def main(args: Array[String]): Unit = {
    //logger
    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

    //Check command line arguments
    // Logger to check if data is there
    if (args.length == 0) {
      logger.error("Usage: RDD filename")
      System.exit(1)
    }

    //Create Spark Context
    val sparkAppConf = new SparkConf().setAppName("RDD").setMaster("local[3]")
    //Create Spark Context
    val sparkContext = new SparkContext(sparkAppConf)

    //Read from datafile
    val linesRDD = sparkContext.textFile(args(0))
    //Partition to RDD
    val partitionedRDD = linesRDD.repartition(2)

    val colsRDD = partitionedRDD.map(line => line.split(",").map(_.trim))
    val selectRDD = colsRDD.map(cols => SurveyRecord(cols(1).toInt, cols(2), cols(3), cols(4)))
    val filteredRDD = selectRDD.filter(row => row.Age < 40)

    val kvRDD = filteredRDD.map(row => (row.Country,1))
    val countRDD = kvRDD.reduceByKey((v1, v2) => v1 + v2)

    logger.info(countRDD.collect().mkString(","))

    sparkContext.stop()
  }
}
