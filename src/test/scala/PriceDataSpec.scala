package org.example

import org.apache.spark.sql.SparkSession
import org.example.datasets.{Joiner, PriceData, ZacksFCData}
import org.example.fields.FieldsZacksFC
import org.scalatest.BeforeAndAfter
import org.scalatest.wordspec.AnyWordSpec

class PriceDataSpec extends AnyWordSpec with BeforeAndAfter {

  implicit var testSession: SparkSession = _
  val zackFc_filename = "/Users/dali/workspace/zacks/data/ZACKS_FR_b5e8ff4a9fdecae8e32b4e0e9c13933e.csv"
  val sharadar_filename = "/Users/dali/workspace/zacks/data/SHARADAR_SFP_2_fb4f5d2244276f3cfeca03f46b122d99.csv"

  val zackFc_sample = "zacks_FC_sample.csv"
  val sharadar_sample = "sharadar_SEP_sample.csv"

  before {
    testSession = SparkSession.builder()
      .appName("TestSpark1")
      .config("spark.master", "local")
      .getOrCreate()
  }

  after {
    testSession.stop()
  }
  "A PriceData object" should {
    "read dataframe" in {
      val priceData = new PriceData(sharadar_filename)
      priceData.UniqueTickers.show()

    }
  }

  "A ZacksFCData object" should {
    "read dataframe" in {
      val zacksFcData = new ZacksFCData(zackFc_filename)
      zacksFcData.UniqueTickers.show()

    }
  }

  "A join object" should {
    "join and give common tickers" in {
      val zacksFcData = new ZacksFCData(zackFc_filename)
      val priceData = new PriceData(sharadar_filename)
      val distincts = Joiner.GetCommonTickers(priceData.df, zacksFcData.df)
//        .select(col = "ticker", "m_ticker")
//        .dropDuplicates(Seq("ticker"))
//        .cache()
      distincts.show(false)
      println(s"Number of common tickers: ${distincts.count}")
    }
  }

    "join on filing date" in {
      val zacksFcData = new ZacksFCData(zackFc_sample)
      val priceData = new PriceData(sharadar_sample)
      println(s"Number of zacksFC lines: ${zacksFcData.df.count}")
      println(s"Number of Sharadar lines: ${priceData.df.count}")
      val joined = Joiner.JoinOnFilingDate(priceData.df, zacksFcData.df)
        .cache()
      //        .select(col = "ticker", "m_ticker")
      //        .dropDuplicates(Seq("ticker"))
      //        .cache()
      joined.show(false)
      println(s"Number of common tickers: ${joined.count}")
  }

  "join ALL on filing date" in {
    val zacksFcData = new ZacksFCData("/Users/dali/workspace/zacks/data/ZACKS_FC_2a2927dcd04466d0527b471825f560fa.csv")
    val priceData = new PriceData("/Users/dali/workspace/zacks/data/SHARADAR_SEP_2_0afbc06bfa7d2d5ebd28c43e0940ec30.csv")
    println(s"Number of zacksFC lines: ${zacksFcData.df.count}")
    println(s"Number of Sharadar lines: ${priceData.df.count}")
    val joined = Joiner.JoinOnFilingDate(priceData.df, zacksFcData.df)
      .cache()
    //        .select(col = "ticker", "m_ticker")
    //        .dropDuplicates(Seq("ticker"))
    //        .cache()
    joined
      .coalesce(1)
      .write.format("csv")
      .option("header", "true")
      .save("zacks_FC_with_prices.csv")
    println(s"Number of joined rows: ${joined.count}")
  }

  "join ALL on filing date with normalized" in {
    val zacksFcDF = new Experiment(FieldsZacksFC.indicators, "transformation", testSession).PrepareDF
    val priceData = new PriceData("/Users/dali/workspace/zacks/data/SHARADAR_SEP_2_0afbc06bfa7d2d5ebd28c43e0940ec30.csv")
    println(s"Number of zacksFC lines: ${zacksFcDF.count}")
    println(s"Number of Sharadar lines: ${priceData.df.count}")
    val joined = Joiner.JoinOnFilingDate(priceData.df, zacksFcDF)
      .cache()
    //        .select(col = "ticker", "m_ticker")
    //        .dropDuplicates(Seq("ticker"))
    //        .cache()
    joined
      .coalesce(1)
      .write.format("csv")
      .option("header", "true")
      .save("zacks_FC_with_prices_normalized.csv")
    println(s"Number of joined rows: ${joined.count}")
  }

}
