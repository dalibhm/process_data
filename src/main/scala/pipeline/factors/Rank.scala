package org.example
package pipeline.factors

import pipeline.{ComputableTerm, InputTerm}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.rank


//TODO: zipline integrates ranking in the Factor Class, See what are the benefits,
//      MAYBE in Scala it makes sense to have it as a factor of its own

class Rank(val input: InputTerm) extends ComputableTerm {
  override val tableName = s"max_${input.tableName}"
  override val inputs: Option[Seq[InputTerm]] = Some(Seq(input))
//  override val _params: Option[Seq[Double]] = None

//  override def _compute(inputs: Seq[Double], dates: Seq[String], assets: Seq[Any], mask: Seq[Any]): Unit = ???

  override def _compute(df: DataFrame): DataFrame = {
    val crossSection = Window
      .partitionBy(datesColName)
      .orderBy(input.colName)

    df.withColumn(colName, rank().over(crossSection))
  }

  override val colName: String = s"rank_${input.colName}"
}
