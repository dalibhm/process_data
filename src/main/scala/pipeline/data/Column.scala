package org.example
package pipeline.data

import pipeline.InputTerm

case class Column(override val colName: String,
                  dataset: Dataset) extends InputTerm {
  override val tableName: String = dataset.name

}