package org.example
package pipeline.loaders

import org.apache.spark.sql.SparkSession

class EquityLoader(val fileName: String, spark: SparkSession) {
  val data = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(fileName)
}

object EquityLoaderTest extends App {

  val spark = SparkSession.builder()
    .appName("EquityLoaderTest")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val fileName = getClass.getResource("/data/sharadar_SEP_sample.csv").getPath
  val df = new EquityLoader(fileName, spark).data
  df.show()
}
