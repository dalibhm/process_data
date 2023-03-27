package org.example

import org.apache.spark.sql.SparkSession

object SplitDataFrame extends App{
  /**
   * Reads a dataframes and splits it into train, dev and test dataframes
   */
  val spark = SparkSession.builder()
    .appName("ExtractSample")
    .config("spark.master", "local")
    .getOrCreate()

  val inputFile = "normalized_with_prices.csv"


  val splits = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(inputFile)
    .randomSplit(Array(0.8, 0.1, 0.1))

  val splitNames = Seq("train", "dev", "test")
  for(i <- Seq(0, 1, 2))
    splits(i)
      .coalesce(1)
      .write.format("csv")
      .option("header", "true")
      .save(s"normalized_${splitNames(i)}.csv")

}
