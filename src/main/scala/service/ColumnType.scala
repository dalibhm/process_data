package org.example
package service

trait ColumnType
case object InputColumn extends ColumnType

abstract class FactorColumn extends ColumnType {
  val inputs: Seq[ColumnDef]
}

case class ReturnColumn(inputColumn: ColumnDef) extends FactorColumn {
  override val inputs: Seq[ColumnDef] = Seq(inputColumn)
}

case class DailyForwardReturns(inputColumn: ColumnDef, offset: Int) extends FactorColumn {
  override val inputs: Seq[ColumnDef] = Seq(inputColumn)
}

case class MovingAverage(inputColumn: ColumnDef, lookBackPeriod : Int) extends FactorColumn {
  override val inputs: Seq[ColumnDef] = Seq(inputColumn)

}


