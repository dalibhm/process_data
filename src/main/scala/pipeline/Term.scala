package org.example
package pipeline

import utils.DefaultValue

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.col

trait Term {
  val tableName: String
  val tickersColName: String = "ticker"
  val datesColName: String = "date"
}

trait InputTerm extends Term {
  val colName: String
}


object Term {
  val NotSpecified = DefaultValue("NotSpecified", "Used for Term default")
}

abstract class ComputableTerm extends Term with InputTerm {
  val inputs: Option[Seq[InputTerm]] = None
  val params: Option[Seq[Double]] = None

  // dataframe in with input -> DataFrame ou with output
  def _compute(df: DataFrame): DataFrame // , dates: Seq[String], assets: Seq[Any], mask:Seq[Any]): Unit



  def compute: DataFrame => DataFrame = {
    (df: DataFrame) =>
    {
      val dfWithInputs = {
        inputs.get
        .filter(_.isInstanceOf[ComputableTerm])
        .map(_.asInstanceOf[ComputableTerm])
        .foldLeft(df) {
          (result, computableTerm) =>
            println(computableTerm.colName)
            computableTerm._compute(result)
        }
      }
      _compute(dfWithInputs)
    }
  }

//  def Filter(cond: (ComputableTerm) => Boolean): BooleanTerm = {
//    BooleanTerm(this, other, _ < _)
//  }
}

case class BooleanTerm(val left: InputTerm, val right: InputTerm, op: (Column, Column) => Column) extends ComputableTerm {

  override val inputs: Option[Seq[InputTerm]] = Some(Seq(left, right))

  override def _compute(df: DataFrame): DataFrame =
    df.withColumn(colName, op(col(left.colName),  col(right.colName)))

  override val colName: String = s"${left.colName}_${op.toString()}_${right.colName}"
  override val tableName: String = "table name"
}

trait Factor extends ComputableTerm {
  def demean(): Unit
  def zscore(): Unit
  def rank(): Unit
}