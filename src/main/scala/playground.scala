package org.example

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object playground extends App{
  val spark = SparkSession.builder()
    .appName("ExtractSample")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val filename = "/Users/dali/scala-projects/spark-big-data/src/main/resources/data/sharadar_SEP_sample.csv"

  val df = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(filename)

  df.printSchema()

  df.groupBy("ticker")
//    .agg(count("date")).show()

  df.groupBy("date")
    .agg(count("ticker"))
    .sort("date")
//    .show()

//  val windowSpec = Window.partitionBy("date")
//    .orderBy("close")
////    .rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
//  val closeRank = rank().over(windowSpec)
//  val percentile = ntile(4).over(windowSpec)
//  val rankedDF = df
//    .withColumn("rank", closeRank)
//    .withColumn("percentile", percentile)
//    .withColumn("date_1", to_date(col("date")))
////    .show()
//
//  val timeWindowSpec = Window.partitionBy("ticker")
//    .orderBy(col("date").asc)
//    .rangeBetween(Window.currentRow - 7, Window.currentRow)

//  val rowNumber = row_number().over(timeWindowSpec)
//  val sum_7 = rowNumber()
//  val rollingSum = sum("close").over(timeWindowSpec)
//  rankedDF
////    .withColumn("rowNumber", rowNumber)
//    .withColumn("rollingSum", rollingSum)
//    .show()

  val colName: String = "volume"
  val aggregationPeriod: Int = 30
  val outputColumn = "volume_30"

  val timeWindowSpec = Window.partitionBy("ticker")
    .orderBy(col("date").asc)
    .rowsBetween(Window.currentRow - 1, Window.currentRow)

  val agg =  sum(colName).over(timeWindowSpec)
  df.withColumn(outputColumn, agg).show()

  def computeMean(inputCol: String, meanPeriod: Int, outputCol: String): DataFrame => DataFrame = {
    val timeWindowSpec = Window.partitionBy("ticker")
      .orderBy(col("date").asc)
      .rowsBetween(Window.currentRow - meanPeriod, Window.currentRow)

    val mean = functions.mean(inputCol).over(timeWindowSpec)

    df => df.withColumn(outputCol, mean)
  }

//  df.transform(computeMean("volume", 30, "volume_avg_30")).show()


  def computeLead(inputCol: String, leadPeriod: Int, outputCol: String): DataFrame => DataFrame = {
    val timeWindowSpec = Window.partitionBy("ticker")
      .orderBy(col("date").asc)
//      .rowsBetween(Window.currentRow - leadPeriod, Window.currentRow)

    val lead = functions.lead(inputCol, offset = leadPeriod).over(timeWindowSpec)

    df => df.withColumn(outputCol, lead)
  }

  var dfTransformed = df

  for(lead <- Range(1, 180)) {
    dfTransformed = dfTransformed.transform(computeLead("close", lead, s"forward_close_$lead"))
  }
  dfTransformed.show()
  //    .show()

  def intersectionQuantiles(inputCol: String, numberOfQuantiles: Int, outputCol: String): DataFrame => DataFrame = {
    val timeWindowSpec = Window.partitionBy("date")
      .orderBy(col(inputCol).asc)
    //      .rowsBetween(Window.currentRow - leadPeriod, Window.currentRow)

    val quantiles = functions.ntile(numberOfQuantiles).over(timeWindowSpec)

    df => df.withColumn(outputCol, quantiles)
  }

  df.transform(intersectionQuantiles("close", 5, "close_bucketed"))
    .show()

}
