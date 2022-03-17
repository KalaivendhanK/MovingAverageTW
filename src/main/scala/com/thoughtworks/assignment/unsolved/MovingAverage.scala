package com.thoughtworks.assignment.unsolved

import com.thoughtworks.assignment.SparkSessionBuilder
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object OutputWriter {
  def write(outDF: DataFrame, path: String)= outDF.coalesce(1).write.option("header","true").csv(path)
}

case class MovingAverage(sparkSession: SparkSession, stockPriceInputDir: String, size: Integer) {
  def calculate() : DataFrame = {
    val input: DataFrame = sparkSession.read.option("header","true").csv(stockPriceInputDir).na.drop()
    input.createOrReplaceTempView("input_table")
    sparkSession.sql(
      f"""
        |select
        | cast(stockId as int) as stockId
        |,cast(timeStamp as int) as timeStamp
        |,cast(stockPrice as double) as stockPrice
        |,cast(round(moving_average,2) as double) as moving_average
        |from (
        |   select *
        |   ,avg(cast(stockprice as double)) over (
        |       partition by cast(stockId as int) order by cast(timestamp as int) asc
        |       rows between ${size - 1} preceding and current row
        |   ) as moving_average from input_table
        |   where trim(stockprice) != "null" and trim(timestamp) != "null" and trim(stockid) != "null"
        | )a
        | where cast(timestamp as int) >= ${size}
        | order by 1,2
        |""".stripMargin)
  }
}
object MovingAverage {
  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSessionBuilder.build

    if (args.length != 3) {
      println("Correct Usage[MovingAverage <size> <stockPriceInputDir> <outputDir>]")
    }

    val outputDF = MovingAverage(spark, args(1), Integer.parseInt(args(0))).calculate()
    OutputWriter.write(outputDF, args(2))
  }

}

case class MovingAverageWithStockInfo(sparkSession: SparkSession, stockPriceInputDir: String, stockInfoInputDir: String, size: Integer) {
  def calculate() : DataFrame = {
    val stockInfo = sparkSession
      .read
      .option("header","true")
      .csv(stockInfoInputDir)

    val movingAverage = MovingAverage(sparkSession,stockPriceInputDir,size)
      .calculate()

    val joinedStockInfo = movingAverage
      .join(stockInfo, stockInfo("stockId") === movingAverage("stockId"), "left")
      .drop(stockInfo("stockId"))
      .selectExpr("stockId", "timeStamp", "stockPrice", "moving_average", "stockName", "stockCategory")
      .orderBy(col("stockId").asc,col("timestamp").asc)

//    joinedStockInfo.show(10,false)

    joinedStockInfo
  }
  def calculateForAStock(stockId: String) : DataFrame = calculate().filter(col("stockid") === stockId)
}

object MovingAverageWithStockInfo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSessionBuilder.build

    if (args.length >= 4) {
      println("Correct Usage[MovingAverage <size> <stockPriceInputDir> <stockInfoInputDir> <outputDir> [<stockId>]]")
    }

    val movingAverageWithStockInfo = MovingAverageWithStockInfo(spark, args(1), args(2), Integer.parseInt(args(0)))
    val outputDF = args.lift(4) match {
      case Some(stockId) => movingAverageWithStockInfo.calculateForAStock(stockId)
      case _ => movingAverageWithStockInfo.calculate()
    }

    OutputWriter.write(outputDF, args(3))
  }
}

