package enesuguroglu.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.spark_partition_id
import org.apache.spark.sql.{SaveMode, SparkSession}

object DataSink extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    // Create Spark Session
    val spark = SparkSession.builder()
      .appName("Spark Schema Demo")
      .master("local[3]")
      .getOrCreate()

    // Read parquet data from the folder
    val df = spark.read
      .format("parquet")
      .option("path", "dataSource/")
      .load()

    // Checking how many records on each partition
    df.groupBy(spark_partition_id()).count().show()

    // Checking parititon number before and after writing
    logger.info("Num Partitions before:" + df.rdd.getNumPartitions)
    val partitioneddf = df.repartition(1)
    logger.info("Num Partitions after:" + partitioneddf.rdd.getNumPartitions)

    partitioneddf.groupBy(spark_partition_id()).count().show()

    partitioneddf.write
      .format("avro")
      .mode(SaveMode.Overwrite)
      .option("path", "dataSink/avro/")
      .save()

    logger.info("Finished Partitioning")
    spark.stop()

  }

}
