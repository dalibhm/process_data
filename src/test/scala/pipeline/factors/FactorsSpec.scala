package org.example
package pipeline.factors

import pipeline.data.{Column, Dataset, EquityPricing}

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.BeforeAndAfter
import org.scalatest.wordspec.AnyWordSpec

class FactorsSpec extends AnyWordSpec with BeforeAndAfter {
  implicit var testSession: SparkSession = _
  var testDF: DataFrame = _

  before {
    testSession = SparkSession.builder()
      .appName("TestFactorSpec")
      .config("spark.master", "local")
      .getOrCreate()

    val sc = testSession.sparkContext

    val schema = new StructType(Array(
      new StructField("ticker", StringType, true),
      new StructField("date", StringType, true),
      new StructField("s1", DoubleType, true),
      new StructField("s2", DoubleType, true),
      new StructField("s3", DoubleType, true),
    ))

    val rdd = sc.parallelize(Seq(
      Row("ticker1", "2020-02-10", 1.0, 34.0, 12.0),
      Row("ticker1", "2020-02-11", -0.5, 35.0, 9.0),
      Row("ticker1", "2020-02-12", 2.0, 34.0, 4.0),
      Row("ticker2", "2020-02-10", 1.0, 33.0, 17.0),
      Row("ticker2", "2020-02-11", 50.0, 34.5, 22.0),
    ))

    testDF = testSession.createDataFrame(rdd, schema)

    //.toDF("S1", "S2", "S3")
  }

  after {
    testSession.stop()
  }

  object TestDataset extends Dataset {
    val s1 = Column("S1", this)
    val s2 = Column("S2", this)
    val s3 = Column("S3", this)
    override val name: String = "testDataset"
    override val columns: Seq[Column] = Seq(s1, s2, s3)
  }

  "A Factor object" should {
    "work" in {
      val ds = EquityPricing()
      val cols = ds.columns
      // EQUALITY HERE DOES NOT MEAN THE INSTANCES ARE THE SAME
      assert(cols == List(Column("open", ds), Column("high", ds), Column("low", ds), Column("close", ds), Column("volume", ds)))

    }
  }

  "DailyReturns factor" should {
    "compute returns" in {
      val returns = new DailyReturns(TestDataset.s1)
      returns._compute(testDF).show()

      /**
       * +-------+----------+----+----+----+-------+
          | ticker|     dates|  s1|  s2|  s3|returns|
          +-------+----------+----+----+----+-------+
          |tticker|2020-02-10| 1.0|34.0|12.0|   null|
          |tticker|2020-02-11|-0.5|35.0| 9.0|   -1.5|
          |tticker|2020-02-12| 2.0|34.0| 4.0|    2.5|
          |tticker|2020-02-13| 1.0|33.0|17.0|   -1.0|
          |tticker|2020-02-14| 5.0|34.5|22.0|    4.0|
          +-------+----------+----+----+----+-------+
       */
    }
    "compute 2 returns" in {
      val returns_1 = new DailyReturns(TestDataset.s1)
      val returns_2 = new DailyReturns(TestDataset.s2)
      testDF
        .transform(returns_1.compute)
        .transform(returns_2.compute)
        .show()
    }
    "compute 2 ranks" in {
      val rank_1 = new Rank(TestDataset.s1)
      val rank_2 = new Rank(TestDataset.s2)
      testDF
        .transform(rank_1.compute)
        .transform(rank_2.compute)
        .show()
    }
    "compute ranks on returns" in {
      val returns = new DailyReturns(TestDataset.s1)
      val rank = new Rank(returns)
      testDF
        .transform(rank.compute)
        .show()
    }
    "compare MA on returns" in {
      val returns = new DailyReturns(TestDataset.s1)
      val returnsMA2 = new MovingAverage(returns, lookBackPeriod = 2)
      testDF
        .transform(returnsMA2.compute)
        .show()
    }


    "compare 2 MA on returns" in {
      val returns = new DailyReturns(TestDataset.s1)
      val returnsMA1 = new MovingAverage(returns, lookBackPeriod = 1)
      val returnsMA2 = new MovingAverage(returns, lookBackPeriod = 2)
      testDF
        .transform(returnsMA1.compute)
        .transform(returnsMA2.compute)
        .show()
    }
  }
}
