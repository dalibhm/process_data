package org.example
package pipeline.data

abstract class Dataset {
  val name: String
  val columns: Seq[Column]
}


class EquityPricing extends Dataset {
  val open = Column("open", this)
  val high = Column("high", this)
  val low = Column("low", this)
  val close = Column("close", this)
  val volume = Column("volume", this)

  override val columns: Seq[Column] = Seq(open, high, low, close, volume)
  override val name: String = "EquityPricing"
}



object EquityPricing {
  val equityPricing = new EquityPricing()
  def apply(): EquityPricing = equityPricing
}