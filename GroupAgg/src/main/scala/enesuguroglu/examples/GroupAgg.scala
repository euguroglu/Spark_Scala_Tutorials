package enesuguroglu.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GroupAgg extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Group Agg Demo")
      .master("local[3]")
      .getOrCreate()

    spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

    val invoiceDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("C:\\Users\\PC\\Documents\\Jupyter\\Udemy\\Spark_Scala\\Program\\AggDemo\\data")

    val aggDF = invoiceDF.withColumn("InvoiceDate", to_date(col("InvoiceDate"), "dd-MM-yyyy"))
      .where("year(InvoiceDate) == 2010")
      .withColumn("WeekNumber", weekofyear(col("InvoiceDate")))
      .groupBy("Country", "WeekNumber")
      .agg(countDistinct("InvoiceNo").as("NumInvoices"),
        sum("Quantity").as("TotalQuantity"),
        expr("round(sum(Quantity * UnitPrice),2) as InvoiceValue"))

    aggDF.show(10)

    aggDF.coalesce(1)
      .write
      .format("parquet")
      .mode("overwrite")
      .save("output")


  }

}
