package org.example
package common

import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadTable {
  def ReadDF(sparkSession: SparkSession, filename: String): DataFrame =
    sparkSession.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(filename)
}
