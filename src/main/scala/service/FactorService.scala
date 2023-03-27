package org.example
package service

import scala.util.Try

trait DataRepository

trait FactorService[Repository] {

  def add(tableDef: TableDef, columnDef: ColumnDef): Try[TableDef] = tableDef.addFactor(columnDef)

  def run: Unit

}

//object FactorServiceImpl extends FactorService[PriceData, DataRepository] {
//  override def add(dataset: PriceData, factor: ComputableTerm): Try[PriceData] =
//    dataset.copy()
//
//  override def run: Unit = ???
//}
