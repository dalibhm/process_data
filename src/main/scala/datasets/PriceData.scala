package org.example
package datasets

import org.apache.spark.sql.{DataFrame, SparkSession}

case class PriceData(override val filename: String)(implicit sparkSession: SparkSession) extends Dataset {
  override val fields: Array[String] = Array(
    "ticker", "date", "open", "high", "low", "close", "volume", "closeadj", "closeunadj", "lastupdated"
  )

  override val df = sparkSession.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(filename)

  override def UniqueTickers(): DataFrame = {
    df.select(col="ticker").distinct()
  }
}
