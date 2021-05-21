package enesuguroglu.examples

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import enesuguroglu.examples._02SparkUnitTestCase.{countByCountry, loadSurveyDF}

import scala.collection.mutable

class Test1 extends FunSuite with BeforeAndAfterAll{

  @transient var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("SparkTest1")
      .master("local[3]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()

  }

  test("Data File Loading") {
    val sampleDF = loadSurveyDF(spark, "data/sample.csv")
    val rCount = sampleDF.count()
    assert(rCount==9, "record count should be 9")
  }

  test("Count by Country") {
    val sampleDF = loadSurveyDF(spark, "data/sample.csv")
    val countDF = countByCountry(sampleDF)
    val countryMap = new mutable.HashMap[String, Long]
    countDF.collect().foreach(r => countryMap.put(r.getString(0), r.getLong(1)))

    assert(countryMap("United States") == 4, ":- Count for United States sjould be 6")
    assert(countryMap("Canada") == 2, ":- Count for Canada should be 2")
    assert(countryMap("United Kingdom") == 1, ":- Count for United Kingdom should be 1")
  }
}
