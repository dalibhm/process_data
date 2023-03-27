package org.example
package service


import scala.util.Try




case class ColumnDef(columnName: String,
                     columnType: ColumnType) {
  def inputs: Seq[ColumnDef] = columnType match {
    case factor:FactorColumn => factor.inputs
    case _ => Seq.empty
  }
}




case class TableDef(inputColumns: Seq[ColumnDef],
                    factorColumns: Seq[ColumnDef] = Seq.empty) {

  def addFactor(factor: ColumnDef): Try[TableDef] = Try {
    copy(factorColumns = factor +: factorColumns)
  }

  override def toString: String = {
    s"""TableDef:
       | ${inputColumns.map(i => s"${i.columnName} : ${i.columnType}")}
       | ${factorColumns.map(f => s"${f.columnName} : ${f.columnType}")}
       |""".stripMargin
  }
}





object TestTableDef extends App {
  val priceCol = ColumnDef("price", InputColumn)
  val table = TableDef(Seq(priceCol))
    .addFactor(ColumnDef("return", ReturnColumn(priceCol)))

  println(table)
}


