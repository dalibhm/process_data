package org.example

import org.apache.spark.sql.SparkSession
import org.example.fields.FieldsZacksFC

object ReconcileTickersSFPvsFC extends App{
  /**
   * This file compares the number of tickers between  Zacks FC and Sharadar SEP tables data
   */

  val spark = SparkSession.builder()
    .appName("ExtractSample")
    .config("spark.master", "local")
    .getOrCreate()

  val zacksFC = "/Users/dali/workspace/zacks/data/ZACKS_FC_2a2927dcd04466d0527b471825f560fa.csv"
  val sharadarSDP = "/Users/dali/workspace/zacks/data/SHARADAR_SEP_2_0afbc06bfa7d2d5ebd28c43e0940ec30.csv"

  val zacksDF = new Experiment(FieldsZacksFC.indicators, "transformation", spark).PrepareDF

  val sharadarDF = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(sharadarSDP)

  zacksDF.printSchema()
  sharadarDF.printSchema()

  val zacksDistinctTickersDF = zacksDF.select("ticker").distinct().cache()
  val sharadarDistinctTickersDF = zacksDF.select("ticker").distinct().cache()

//  zacksDistinctTickersDF
//    .coalesce(1)
//    .write.format("csv")
//    .option("header", "true")
//    .save("zacks_tickers.csv")
//
//  sharadarDistinctTickersDF
//    .coalesce(1)
//    .write.format("csv")
//    .option("header", "true")
//    .save("sharadar_tickers.csv")

  val joined = sharadarDistinctTickersDF
    .join(sharadarDistinctTickersDF,
      Seq("ticker"),
      "inner")
    .cache()

  joined.show(false)

  val commonTickersCount = joined.count()
  val zacksTickersCount = zacksDistinctTickersDF.count()
  val sharadarTickersCount = sharadarDistinctTickersDF.count()
  val zacksLinesCount = zacksDF.count()
  val sharadarLinesCount = sharadarDF.count()

  println(s"${commonTickersCount} common tickers")
  println(s"${zacksTickersCount} Zacks tickers")
  println(s"${sharadarTickersCount} Sharadar tickers")

  println(s"${zacksLinesCount} Zacks lines")
  println(s"${sharadarLinesCount} Sharadar lines")
}
