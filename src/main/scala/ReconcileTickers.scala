package org.example

import org.apache.spark.sql.SparkSession

object ReconcileTickers extends App{
  /**
   * This file compares the number of tickers between  Zacks and Sharadar Master tables data
   */

  val spark = SparkSession.builder()
    .appName("ExtractSample")
    .config("spark.master", "local")
    .getOrCreate()

  val zacksMT = "/Users/dali/workspace/zacks/data/ZACKS_MT_477bbb97a2ffc86235b009cc18ac2435.csv"
  val sharadarMT = "/Users/dali/workspace/zacks/data/SHARADAR_TICKERS_6cc728d11002ab9cb99aa8654a6b9f4e.csv"

  val zacksDF = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(zacksMT)

  val sharadarDF = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(sharadarMT)

  zacksDF.printSchema()
  sharadarDF.printSchema()

  sharadarDF.join(zacksDF,
      Seq("ticker"),
      "inner")
    .cache()
    .show(false)

  val commonTickersCount = sharadarDF.join(zacksDF,
      Seq("ticker"),
      "inner")
    .count()

  println(s"${commonTickersCount} common tickers")
  println(s"${zacksDF.count()} Zacks tickers")
  println(s"${sharadarDF.count()} Sharadar tickers")
}
