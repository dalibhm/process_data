package org.example

import fields.FieldsZacksFC

import org.apache.spark.sql.SparkSession
import org.example.common.ReadTable
import org.scalatest.BeforeAndAfter
import org.scalatest.wordspec.AnyWordSpec

class ExperimentSpec extends AnyWordSpec with BeforeAndAfter {

  implicit var testSession: SparkSession = _

  before {
    testSession = SparkSession.builder()
      .appName("TestSpark1")
      .config("spark.master", "local")
      .getOrCreate()
  }

  after {
    testSession.stop()
  }
  "An Experiment" should {
    "work" in {
      val fields = FieldsZacksFC.indicators
      val zacks = new Experiment(fields, "mock", testSession).PrepareDF
      val withPrices = ReadTable.ReadDF(testSession, "zacks_FC_with_prices.csv")

      withPrices.select("m_ticker", "per_type", "per_end_date", "date", "close", "lead1Day", "lead5Days","lag1Day","lag5Days").show(false)

      val joined = zacks.join(
        withPrices.
          select("m_ticker", "per_type", "per_end_date", "date", "close", "lead1Day", "lead5Days","lag1Day","lag5Days"),
        Seq("m_ticker", "per_type", "per_end_date")).cache()
//
//      joined.show(false)

//      zacks.printSchema()

      joined
        .coalesce(1)
        .write.format("csv")
        .option("header", "true")
        .save("normalized_with_prices.csv")

    }
  }
}
