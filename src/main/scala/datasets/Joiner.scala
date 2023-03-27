package org.example
package datasets

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, lead}

object Joiner {
  def GetCommonTickers(stocksDF: DataFrame, fundamentalsDF: DataFrame): DataFrame =
    stocksDF.select("ticker").distinct()
      .join(fundamentalsDF.select("ticker").distinct(),
        Seq("ticker"),
        "inner")

  def JoinOnFilingDate(stocksDF: DataFrame, fundamentalsDF: DataFrame): DataFrame = {
    val windowSpec = Window
      .partitionBy(stocksDF("ticker"))
      .orderBy(stocksDF("date"))
//      .rowsBetween(start = -1, end = 30) // can use rangeBetween for range instead

    val lead1Day = lead(col("close"), 1).over(windowSpec)
    val lead5Day = lead(col("close"), 5).over(windowSpec)
    val lag1Day = lag(col("close"), 1).over(windowSpec)
    val lag5Day = lag(col("close"), 5).over(windowSpec)

    val stocksWithLaggedPrices = stocksDF
      .select(
        col("ticker"),
        col("date"),
        col("close"),
        lead1Day.as("lead1Day"),
        lead5Day.as("lead5Days"),
//    close_lead_20.alias("close_lead_20"),
//    close_lead_60.alias("close_lead_60"),
        lag1Day.as("lag1Day"),
        lag5Day.alias("lag5Days"),
//    close_lag_20.alias("close_lag_20"),
//    close_lag_60.alias("close_lag_60"),
//    close_lag_120.alias("close_lag_120"),
//    close_lag_240.alias("close_lag_240"),
//    closeRank.alias("closeRank"),
//    closeDenseRank.alias("closeDenseRank")
    ).withColumnRenamed("ticker", "ticker_S")

    fundamentalsDF
      .join(stocksWithLaggedPrices)
      .where(fundamentalsDF("ticker") === stocksWithLaggedPrices("ticker_S")
        && fundamentalsDF("filing_date") === stocksWithLaggedPrices("date"))

  }
}
