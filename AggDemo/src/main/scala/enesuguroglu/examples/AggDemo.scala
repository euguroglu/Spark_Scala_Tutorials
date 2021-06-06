package enesuguroglu.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object AggDemo extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Agg Demo")
      .master("local[3]")
      .getOrCreate()

    val invoiceDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("data/invoices.csv")

    val df = invoiceDF.select(
      count("*").as("Count *"),
      sum("Quantity").as("TotalQuantity"),
      avg("UnitPrice").as("AvgPrice"),
      countDistinct("InvoiceNo").as("CountDistinct")
    )

    df.show()
    // Note that count field is not counting nulls while count(1) or count(*) counts all values
    val df2 = invoiceDF.selectExpr(
      "count(1) as `count 1`",
      "count(StockCode) as `count field`",
      "sum(Quantity) as TotalQuantity",
      "avg(UnitPrice) as AvgPrice"
    )

    df2.show()

    invoiceDF.createOrReplaceTempView("sales")
    val summarySQL = spark.sql(
      """
        |SELECT Country, InvoiceNo,
        | sum(Quantity) as TotalQuantity,
        | round(sum(Quantity*UnitPrice),2) as InvoiceValue
        | FROM sales
        | GROUP BY Country, InvoiceNo
        |""".stripMargin)

    summarySQL.show()

    val summarydf = invoiceDF.groupBy("Country", "InvoiceNo")
      .agg(sum("Quantity").as("TotalQuantity"),
        round(sum(expr("Quantity*UnitPrice")),2).as("InvoiceValue"))

    summarydf.show()
  }

}
