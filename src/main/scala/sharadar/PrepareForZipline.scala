package org.example
package sharadar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object PrepareForZipline extends App{

  val spark = SparkSession.builder()
    .appName("PrepareForZipline")
    .config("spark.master", "local[*]")
    .config("spark.memory", "4G")
    .getOrCreate()

  val actionsDF = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(Files.actions)

  actionsDF.printSchema()
  val splitsDF = actionsDF
    .filter(col("action") === "split")
    .select(col("date"), col("ticker"), col("value"))
    .withColumnRenamed("value", "split")
//    .show()

  val ochlvDF = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(Files.sep)

  val withSplitsDF = ochlvDF.join(splitsDF, Seq("ticker", "date"), joinType = "left")

  withSplitsDF
    .na.fill(1, Seq("split"))
//    .coalesce(1)
    .write
    .partitionBy("date")
    .format("csv")
    .option("header", "true")
    .save(s"${Files.DATA_ROOT}/sharadar_to_ingest_partitioned_by_date.csv")


}
