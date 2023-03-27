package org.example
package pipeline.data

import org.scalatest.BeforeAndAfter
import org.scalatest.wordspec.AnyWordSpec

class DatasetSpec extends AnyWordSpec with BeforeAndAfter {


  "A EquityPricingDataset object" should {
    "have columns" in {
      val ds = EquityPricing()
      val cols = ds.columns
      // EQUALITY HERE DOES NOT MEAN THE INSTANCES ARE THE SAME
      assert(cols == Seq(Column("open", ds), Column("high", ds), Column("low", ds), Column("close", ds), Column("volume", ds)))

    }
  }
}
