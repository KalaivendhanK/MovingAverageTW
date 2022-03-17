package com.thoughtworks.assignment

import com.thoughtworks.assignment.unsolved.{MovingAverage, MovingAverageWithStockInfo}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

class MovingAverageTest extends QueryTest with SharedSQLContext {
  import dataFrameMatchers._
  val stockPriceInputDir = "src/test/resources/input/stock_prices"
  val stockPriceDirtyInputDir = "src/test/resources/input/stock_prices_dirty"
  val stockInfoInputDir = "src/test/resources/input/stock_info"
  test("moving average") {
    val movingAverageDF = MovingAverage(spark, stockPriceInputDir, 3).calculate()

    val expectedOutput = spark.read.format("csv").option("header", "true")
      .schema(StructType(
        StructField("stockId", IntegerType) ::
          StructField("timeStamp", IntegerType) ::
          StructField("stockPrice", DoubleType) ::
          StructField("moving_average", DoubleType) ::
          Nil))
      .load("src/test/resources/expectedOutput/moving_average")

    checkAnswer(movingAverageDF, expectedOutput)
    movingAverageDF.shouldHaveSchemaOf(expectedOutput)

  }

  test("moving average with stockInfo"){

    val movingAverageDF = MovingAverageWithStockInfo(spark, stockPriceInputDir, stockInfoInputDir, 3)
      .calculate()

    val expectedOutput = spark.read.format("csv").option("header", "true")
      .schema(StructType(
        StructField("stockId", IntegerType) ::
          StructField("timeStamp", IntegerType) ::
          StructField("stockPrice", DoubleType) ::
          StructField("moving_average", DoubleType) ::
          StructField("stockName", StringType) ::
          StructField("stockCategory", StringType) ::
          Nil))
      .load("src/test/resources/expectedOutput/moving_average_with_stockinfo")

    checkAnswer(movingAverageDF, expectedOutput)
    movingAverageDF.shouldHaveSchemaOf(expectedOutput)

  }

  test("moving average with stockInfo for a stock"){

    val movingAverageDF = MovingAverageWithStockInfo(spark, stockPriceInputDir, stockInfoInputDir, 3)
      .calculateForAStock("101")

    val expectedOutput = spark.read.format("csv").option("header", "true")
      .schema(StructType(
        StructField("stockId", IntegerType) ::
          StructField("timeStamp", IntegerType) ::
          StructField("stockPrice", DoubleType) ::
          StructField("moving_average", DoubleType) ::
          StructField("stockName", StringType) ::
          StructField("stockCategory", StringType) ::
          Nil))
      .load("src/test/resources/expectedOutput/moving_average_with_stockinfo_for_a_stock")

    checkAnswer(movingAverageDF, expectedOutput)
    movingAverageDF.shouldHaveSchemaOf(expectedOutput)
  }

  test("moving average on dirty data") {

    val movingAverageDF = MovingAverage(spark, stockPriceDirtyInputDir, 3).calculate()

    val expectedOutput = spark.read.format("csv").option("header", "true")
      .schema(StructType(
        StructField("stockId", IntegerType) ::
          StructField("timeStamp", IntegerType) ::
          StructField("stockPrice", DoubleType) ::
          StructField("moving_average", DoubleType) ::
          Nil))
      .load("src/test/resources/expectedOutput/moving_average_on_dirty_data")

    checkAnswer(movingAverageDF, expectedOutput)
    movingAverageDF.shouldHaveSchemaOf(expectedOutput)

  }

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.setLogLevel("ERROR")
  }
}
