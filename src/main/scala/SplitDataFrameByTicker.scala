package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array_contains, col, typedLit}

object SplitDataFrameByTicker extends App{
  /**
   * Reads a dataframes and splits it into train, dev and test dataframes
   */

  val spark = SparkSession.builder()
    .appName("ExtractSample")
    .config("spark.master", "local")
    .getOrCreate()

  val inputFile = "normalized_with_prices.csv"


  val df = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(inputFile)

  val tickersSplits = df.select(col("ticker"))
    .distinct()
    .randomSplit(Array(0.8, 0.1, 0.1))

//  for (t <- tickersSplits){
//    t.show(false)
//  }


  val splitNames = Seq("train", "dev", "test")

  for(i <- Seq(0, 1, 2)) {
    df.filter(array_contains(typedLit(tickersSplits(i).rdd.map(r => r(0).toString).collect.toList), col("ticker")))
      .coalesce(1)
      .write.format("csv")
      .option("header", "true")
      .save(s"normalized_by_ticker_${splitNames(i)}.csv")
  }

}
