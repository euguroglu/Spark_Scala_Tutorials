package enesuguroglu

import org.apache.spark.sql.functions.{expr, from_json, _}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object OmsDeliveryReturnedTest {

  def main(args: Array[String]): Unit = {

    //Initiate spark session
    val spark = SparkSession.builder()
      .appName("oms_returned_test")
      .master("local[3]")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //Read data from kafka
    val dataStreamReader = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:5052")
      .option("subscribe", "oms")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    //Read sample data to extract schema
    val df_schema = spark.read
      .format("json")
      .option("inferSchema", "true")
      .load("C:\\Users\\PC\\Documents\\Jupyter\\Udemy\\Spark_Scala\\Program\\OmsDeliveryReturnedTest\\schema\\oms_delivery_returned_products.json")

    //Schema definiton
    val schema = df_schema.schema

    //Deserialization
    val value_df = dataStreamReader.select(from_json(col("value").cast("string"),schema).alias("value"))

    value_df.printSchema()

    //value_df.show()

    //Flattening-1
    val explode_df = value_df.selectExpr("value.id", "value.code", "value.deliveryOrderLines as deliveryorderlines",
      "value.returnedProducts as returnedproducts", "value.receivedAt as receivedat", "value.insertedDate as inserteddate"
    )

    explode_df.printSchema()

    //explode_df.show()

    //Exploding
    val explode_df2 = explode_df
      .withColumn("deliveryorderlinesorderlineexp", explode(col("deliveryorderlines")))
      .withColumn("returnedproductsexp", explode(col("returnedproducts")))
      .drop("deliveryorderlines")
      .drop("returnedproducts")

    explode_df2.printSchema()

    //Flattening-2
    val explode_df3 = explode_df2
      .withColumn("deliveryorderlinesorderlineid", expr("deliveryorderlinesorderlineexp.orderLineId"))
      .withColumn("deliveryorderlinesordernumber", expr("deliveryorderlinesorderlineexp.orderNumber"))
      .withColumn("order_date", expr("deliveryorderlinesorderlineexp.order_date"))
      .withColumn("returnedproductsquantity", expr("returnedproductsexp.quantity"))
      .withColumn("returnedproductssku", expr("returnedproductsexp.sku"))
      .drop("deliveryorderlinesorderlineexp")
      .drop("returnedproductsexp")

    explode_df3.printSchema()


    val df = explode_df3
      .withColumn("deliverycreateddate", expr("""from_unixtime(unix_timestamp(from_utc_timestamp(to_timestamp(unix_timestamp(insertedDate , "yyyy-MM-dd'T'HH:mm")), 'Europe/Istanbul')),'yyyy-MM-dd') """))
      .drop("inserteddate")

    //explode_df3.show()

    //Write streaming data using foreachBatch function
    val query = df.writeStream
      .queryName("OmsDeliveryReturned")
      .foreachBatch(writeBatchData _)
      .option("checkpointLocation", "C:\\Users\\PC\\Documents\\Jupyter\\Udemy\\Spark_Scala\\Program\\OmsDeliveryReturnedTest\\chk").start()
    query.awaitTermination()
  }

  //Define function to write stream data for each batch
  def writeBatchData(batchDF: DataFrame, epoch: Long): Unit = {
    batchDF
      .write.format("console")
      .save()
  }

}
