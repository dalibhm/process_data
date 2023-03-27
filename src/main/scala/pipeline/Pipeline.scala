package org.example
package pipeline

import pipeline.loaders.EquityLoader

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.example.pipeline.data.EquityPricing
import org.example.pipeline.factors.{DailyForwardReturns, DailyReturns, MovingAverage}

class Pipeline(var terms: Seq[ComputableTerm], var names: Seq[String]) {

  def add(term: ComputableTerm, name: String) = {
    terms = terms :+ term
    names = names :+ name
  }
}

class Engine(val loader: EquityLoader){

  def runPipeline(pipeline: Pipeline): DataFrame = {
    pipeline.terms
      .foldLeft(loader.data) {
        (result, computableTerm) =>
          println(computableTerm.colName)
          computableTerm._compute(result)
      }
  }
}


object PipelineTest extends App {

  val spark = SparkSession.builder()
    .appName("PipelineTest")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val fileName = getClass.getResource("/data/sharadar_SEP_sample.csv").getPath
  val dataset = EquityPricing()
  val loader = new EquityLoader(fileName, spark)
  var pipeline = new Pipeline(Seq.empty, Seq.empty)

  pipeline.add(new MovingAverage(dataset.volume, lookBackPeriod = 60), "volume_MA#60")
  pipeline.add(new DailyReturns(dataset.close), "returns")
  pipeline.add(new DailyForwardReturns(dataset.close, offset = 1), "returns_f#1")
  pipeline.add(new DailyForwardReturns(dataset.volume, offset = 2), "returns_f#2")

  val engine = new Engine(loader)
  val result = engine.runPipeline(pipeline)

  result.show()

}
