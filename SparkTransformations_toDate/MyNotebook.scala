// Databricks notebook source
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

def toDateDF(df:DataFrame, fmt:String, fld:String):DataFrame = {
  df.withColumn(fld, to_date(col(fld),fmt))
}

// COMMAND ----------

val mySchema = StructType(List(
  StructField("ID", StringType),
  StructField("EventDate", StringType)
))

val myRows = List(Row("123", "04/05/2020"), Row("124", "4/5/2020"), Row("125", "04/5/2020"), Row("126", "4/05/2020"))
val myRDD = spark.sparkContext.parallelize(myRows, 2)
val myDF = spark.createDataFrame(myRDD, mySchema)

// COMMAND ----------

myDF.printSchema()

// COMMAND ----------

myDF.show()

// COMMAND ----------

val newDF = toDateDF(myDF, "M/d/y", "EventDate")
newDF.show()
