package org.example
package fields

trait Field {
  val code: String
  val name: String
}

case class RowIdentifier(override val code: String, override val name: String) extends Field

case class TextField(override val code: String,
                     override val name: String,
                     val description: String) extends Field

case class IndicatorField(val statementType: String,
                          override val code: String,
                          override val name: String,
                          val description: String,
                          val unit: String) extends Field

case class StandardField(override val name: String, val _type: String, val desciption: String) extends Field {
  override val code = name
  }