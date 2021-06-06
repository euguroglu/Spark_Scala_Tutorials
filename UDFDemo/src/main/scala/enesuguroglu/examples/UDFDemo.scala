package enesuguroglu.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UDFDemo extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      logger.error("Usage: UDFDemo filename")
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .appName("UDF Demo")
      .master("local[3]")
      .getOrCreate()

    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(args(0))

    df.show(10, false)

    //Creating udf with udf function registration
    //This method will not create catalog function and register udf as dataframe function
    //If function will be used as column object expressions that method should be used
    val parseGenderUDF = udf(parseGender(_: String):String)

    //Lets check catalog for observation
    spark.catalog.listFunctions().filter(r => r.name == "parseGenderUDF").show()

    val df2 = df.withColumn("Gender", parseGenderUDF(col("Gender")))

    df2.show(10, false)

    //Creating udf using spark session
    //This method will create catalog function and register udf as sql function
    //If function will be used as sql expression that method should be used
    spark.udf.register("parseGenderUDF", parseGender(_:String): String)

    //Lets check catalog for observation
    spark.catalog.listFunctions().filter(r => r.name == "parseGenderUDF").show()

    val df3 = df.withColumn("Gender", expr("parseGenderUDF(Gender)"))

    df3.show(10, false)
  }

  def parseGender(s:String): String = {
    val femalePattern = "^f$|f.m|w.m".r
    val malePattern = "^m$|ma|m.l".r

    if(femalePattern.findFirstIn(s.toLowerCase).nonEmpty) "Female"
    else if (malePattern.findFirstIn(s.toLowerCase).nonEmpty) "Male"
    else "Unknown"
  }

}
