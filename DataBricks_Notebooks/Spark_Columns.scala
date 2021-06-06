// Databricks notebook source
import org.apache.spark.sql.functions._

// COMMAND ----------

//Checking databricks dataset

// COMMAND ----------

// MAGIC %fs ls /databricks-datasets/

// COMMAND ----------

//Choosing airlines data

// COMMAND ----------

// MAGIC %fs head /databricks-datasets/airlines/part-00000

// COMMAND ----------

val airlinesDF = spark.read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .option("samplingRatio", "0.0001")
  .load("/databricks-datasets/airlines/part-00000")

// COMMAND ----------

//Column string method to reach dataframe columns

// COMMAND ----------

airlinesDF.select("Origin", "Dest", "Distance").show(10)

// COMMAND ----------

//Column object method to reach dataframe columns

// COMMAND ----------

airlinesDF.select(column("Origin"), col("Dest"), $"Distance", 'IsDepDelayed).show(10)

// COMMAND ----------

//String Column Expressions

// COMMAND ----------

airlinesDF.select("Origin", "Dest", "Distance", "Year", "Month", "DayofMonth").show(10)

// COMMAND ----------

airlinesDF.select(col("Origin"), col("Dest"), col("Distance"), expr("to_date(concat(Year,Month,DayofMonth),'yyyyMMdd') as FlightDate")).show(10)

// COMMAND ----------

airlinesDF.selectExpr("Origin", "Dest", "Distance", "to_date(concat(Year,Month,DayofMonth),'yyyyMMdd') as FlightDate").show(10)

// COMMAND ----------

airlinesDF.select(col("Origin"), col("Dest"), col("Distance"), to_date(concat(col("Year"),col("Month"),col("DayofMonth")),"yyyyMMdd").as("FlightDate")).show(10)
