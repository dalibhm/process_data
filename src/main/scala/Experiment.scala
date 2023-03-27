package org.example

import fields.{Field, IndicatorField}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.example.common.ReadTable
import org.example.files.Files

class Experiment(val fields:Seq[Field], val transformations:String, val sparkSession: SparkSession){
  // check which table contains the field
  // read the table if not yet in read
  // select field or transform it
  //
  val zacksFc = ReadTable.ReadDF(sparkSession, Files.zacksFC)
  val zacksSHRS = ReadTable.ReadDF(sparkSession, Files.zacksSHRS)
    .select("m_ticker", "per_type", "per_end_date", "shares_out", "avg_d_shares")
    .withColumnRenamed("avg_d_shares", "AVG_D_SHARES_SHRS")
  val zacksMKTV = ReadTable.ReadDF(sparkSession, Files.zacksMKTV)
    .select("m_ticker", "per_type", "per_end_date", "mkt_val", "ep_val")

  var zacksAll = zacksFc
    .join(zacksSHRS, Seq("m_ticker", "per_type", "per_end_date"))
    .join(zacksMKTV, Seq("m_ticker", "per_type", "per_end_date"))
    .cache()

  def PrepareDF: DataFrame = {
    for (field <- fields) {
      field match {
        case IndicatorField(_, code, _, _, "Millions") => {
          val expression = s"${code} / mkt_val as code"
          zacksAll = zacksAll.withColumn(code, col(code) / col("mkt_val"))
        }
        case _ =>
      }
    }
    zacksAll
  }
}
