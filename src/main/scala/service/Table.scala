package org.example
package service

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{avg, col, lead, log}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

import scala.annotation.tailrec


case class Table(tableDef: TableDef) {

  val evaluated = Seq()

  val inputCols = tableDef.inputColumns.map(c => col(c.columnName))

  val tickersColName: String = "ticker"
  val datesColName: String = "date"

  def timeSection: WindowSpec =
    Window
      .partitionBy(tickersColName)
      .orderBy(col(datesColName).asc)

  val factors = tableDef.factorColumns.map(c => c.columnType)

  def factorNames: Seq[String] = tableDef.factorColumns.map(f => f.columnName)

//  def factorCols: Seq[Column] = factorNames.zip(factors).map{
  def getExpression(columnDef: ColumnDef): Column = columnDef.columnType match {

    case MovingAverage(inputColumn, lookBackPeriod) =>
      val inputCol = inputColumn.columnName
      val res = avg(inputCol)
        .over(timeSection.rowsBetween(Window.currentRow - lookBackPeriod + 1, Window.currentRow))
      res.as(columnDef.columnName)

    case DailyForwardReturns(inputColumn, offset) =>
      val inputCol = inputColumn.columnName
      val res = (
        log(lead(col(inputCol), offset = offset).over(timeSection))
        - log(col(inputCol))
        ) * Literal(100)
      res.as(columnDef.columnName)

    case InputColumn => col(columnDef.columnName)
  }



  @tailrec
  final def evaluate(df: DataFrame,
               toEvaluate: Seq[ColumnDef],
               evaluated: Seq[ColumnDef] = tableDef.inputColumns): DataFrame = {
    def inputAvailable(inputs: Seq[ColumnDef]): Boolean = evaluated.containsSlice(inputs)

    val canEvaluate = toEvaluate
      .filter(c => inputAvailable(c.inputs))

    val selected = (tableDef.inputColumns ++ canEvaluate).map(getExpression)


    val remaining = toEvaluate.filterNot(c => inputAvailable(c.inputs))

    if( remaining.isEmpty ) df.select(selected: _*)
    else evaluate(df.select(selected: _*), toEvaluate, evaluated ++ canEvaluate)

  }

}

object TableTest extends App {
  val testSession = SparkSession.builder()
    .appName("TestFactorSpec")
    .config("spark.master", "local[*]")
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

  val testDF = testSession.createDataFrame(rdd, schema)

  testDF.show(5)

  val ticker = ColumnDef("ticker", InputColumn)
  val date = ColumnDef("date", InputColumn)
  val s1 = ColumnDef("s1", InputColumn)
  val s2 = ColumnDef("s2", InputColumn)
  val s3 = ColumnDef("s3", InputColumn)
  val f1 = ColumnDef("forwardRet", DailyForwardReturns(s2, 1))
  val MaS1 = ColumnDef("MA_s2", MovingAverage(s2, 2))
  val Maf1 = ColumnDef("MA_f1", MovingAverage(f1, 2))

  val tableDef = TableDef(Seq(ticker, date, s1, s2, s3), Seq(MaS1, f1, Maf1))
  val table = Table(tableDef)

  val res = table.evaluate(testDF, Seq(ticker, date, s1, s2, s3, MaS1, f1, Maf1))
  res.show(5)
}
