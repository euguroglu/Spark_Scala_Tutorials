package enesuguroglu.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object MiscDemo extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Misc Demo")
      .master("local[3]")
      .getOrCreate()

    // Manually create dataframe without using case class with toDF function
    val dataList = List(
      ("Enes", 2, 7, 1988),
      ("Dilara", 3, 6, 1988),
      ("Micheal", 3, 6, 1967),
      ("John", 5, 6, 1965),
      ("Jennifer", 1, 6, 1984)
    )

    val df = spark.createDataFrame(dataList).toDF("name", "day", "month", "year")

    df.printSchema()

    val dataList2 = List(
      ("Enes", "2", "7", "1988"),
      ("Dilara", "3","6", "1988"),
      ("Micheal", "3", "6", "1967"),
      ("John", "5", "6", "1965"),
      ("Jennifer", "1", "6", "1984")
    )

    //Creating unique column identifier using monotonicall increasing id function
    val df2 = spark.createDataFrame(dataList2).toDF("name", "day", "month", "year").repartition(3)

    df2.printSchema()

    val finaldf = df2.withColumn("id", monotonically_increasing_id())

    finaldf.show()

    //Using case class with dataframes
    //Using cast in expression
    val dataList3 = List(
      ("Enes", "2", "7", "2002"),
      ("Dilara", "3","6", "81"),
      ("Micheal", "3", "6", "6"),
      ("John", "5", "6", "63"),
      ("Jennifer", "1", "6", "81")
    )

    val df3 = spark.createDataFrame(dataList3).toDF("name", "day", "month", "year").repartition(3)

    val finaldf2 = df3.withColumn("id", monotonically_increasing_id())
      .withColumn("year", expr(
        """
          |case when year < 21 then cast(year as int) + 2000
          |when year < 100 then cast(year as int) + 1900
          |else year
          |end
          |""".stripMargin))

    /* Or we can cas column type at the and using cast function
        val finaldf2 = df3.withColumn("id", monotonically_increasing_id())
      .withColumn("year", expr(
        """
          |case when year < 21 then year + 2000
          |when year < 100 then year + 1900
          |else year
          |end
          |""".stripMargin).cast(IntegerType))
         Note that here schema also will be changed
     */

    finaldf2.show()

    //Best practice

    val finaldf3 = df3.withColumn("id", monotonically_increasing_id())
      .withColumn("day", col("day").cast(IntegerType))
      .withColumn("month", col("month").cast(IntegerType))
      .withColumn("year", col("year").cast(IntegerType))
      .withColumn("year", expr(
        """
          |case when year < 21 then year + 2000
          |when year < 100 then year + 1900
          |else year
          |end
          |""".stripMargin))

    finaldf3.show()

    //Adding column using to_date with string object and string expression method
    val finaldf4 = df3.withColumn("id", monotonically_increasing_id())
      .withColumn("day", col("day").cast(IntegerType))
      .withColumn("month", col("month").cast(IntegerType))
      .withColumn("year", col("year").cast(IntegerType))
      .withColumn("year",
        when(col("year") < 21, col("year") + 2000)
        when(col("year") < 100, col("year") + 1900)
        otherwise col("year")
      )
      .withColumn("dob", expr("to_date(concat(day,'/',month,'/',year),'d/M/y')"))
      .withColumn("dobsecond", to_date(expr("concat(day,'/',month,'/',year)"),"d/M/y"))

    finaldf4.show()
  }

}
