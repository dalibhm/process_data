package org.example
package pipeline.factors

import pipeline.{ComputableTerm, InputTerm}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{avg, col, lag, lead}

abstract class TimeFactor extends ComputableTerm{
  override val tableName: String = "TIME FACTOR"

  def timeSection: WindowSpec =
    Window
      .partitionBy(tickersColName)
      .orderBy(col(datesColName).asc)

}

class DailyReturns(input: InputTerm) extends TimeFactor {
  override val tableName: String = "DAILY_RETURNS_TABLE"
  override val inputs: Option[Seq[InputTerm]] = Some(Seq(input))

  override def _compute(df: DataFrame): DataFrame = {
    df.withColumn(colName,
                  col(input.colName) - lag(input.colName, offset = 1).over(timeSection))
  }

  override val colName: String = s"returns_${input.colName}"
}

class DailyForwardReturns(input: InputTerm, offset:Int) extends TimeFactor {
  override val tableName: String = "DAILY_RETURNS_TABLE"
  override val inputs: Option[Seq[InputTerm]] = Some(Seq(input))

  override def _compute(df: DataFrame): DataFrame = {
    df.withColumn(colName, lead(col(input.colName), offset = offset).over(timeSection) - col(input.colName))
  }

  override val colName: String = s"f${offset}returns_${input.colName}"
}


class MovingAverage(inputTerm: InputTerm, lookBackPeriod: Int) extends TimeFactor {
  override val inputs: Option[Seq[InputTerm]] = Some(Seq(inputTerm))

  override def _compute(df: DataFrame): DataFrame =
    df.withColumn(colName,
      avg(inputTerm.colName).over(timeSection.rowsBetween(Window.currentRow - lookBackPeriod + 1, Window.currentRow))
    )

  override val colName: String = s"MA${lookBackPeriod}_${inputTerm.colName}"
}