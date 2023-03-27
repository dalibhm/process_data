package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array_contains, col, typedLit}

object GenerateSample extends App{

  val spark = SparkSession.builder()
    .appName("ExtractSample")
    .config("spark.master", "local")
    .getOrCreate()

  val zacksFC = "/Users/dali/workspace/zacks/data/ZACKS_FC_2a2927dcd04466d0527b471825f560fa.csv"
  val sharadarSDP = "/Users/dali/workspace/zacks/data/SHARADAR_SEP_2_0afbc06bfa7d2d5ebd28c43e0940ec30.csv"

  val sampleStocks = Seq("ACFN", "ADEP", "ALFVY", "ALXN")

  val zacksDF = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(zacksFC)
    .filter(array_contains(typedLit(sampleStocks), col("ticker")))

  val sharadarDF = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(sharadarSDP)
    .filter(array_contains(typedLit(sampleStocks), col("ticker")))


  zacksDF
    .coalesce(1)
    .write.format("csv")
    .option("header", "true")
    .save("zacks_FC_sample.csv")

  sharadarDF
    .coalesce(1)
    .write.format("csv")
    .option("header", "true")
    .save("sharadar_SEP_sample.csv")

}
