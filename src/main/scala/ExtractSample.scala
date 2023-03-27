package org.example

import org.apache.spark.sql.SparkSession

object ExtractSample extends App {
  val spark = SparkSession.builder()
    .appName("ExtractSample")
    .config("spark.master", "local")
    .getOrCreate()

  val filename = "/Users/dali/workspace/zacks/data/ZACKS_FC_2a2927dcd04466d0527b471825f560fa.csv"

  val df = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(filename)

//  df.show()

  val tickersWithFilingDate = df.filter(df("filing_date").isNotNull)

  val count = tickersWithFilingDate.select("m_ticker", "ticker").distinct().count()
  println(s"tickers with filing date : ${count}")
  println(s"tickers : ${tickersWithFilingDate.select("m_ticker", "ticker").distinct().show()}")
//  df.createOrReplaceTempView("zacks_fc")
//
//
//  val filing_date = spark.sql("SELECT unique(m_ticker) FROM zacks_fc WHERE filing_date is NOT Null")
//
//  val results = filing_date.collect()
//  results.foreach(println)

//    df.describe(["filing_date"]).show()

//  df.printSchema()

  spark.stop()
}