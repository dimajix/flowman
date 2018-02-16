package com.dimajix.flowman.spec.schema

import org.scalatest.FlatSpec
import org.scalatest.Matchers


class IntegerTypeTest extends FlatSpec with Matchers {
    "A IntegerType" should "parse strings" in {
        IntegerType.parse("12").asInstanceOf[Int] should be (12)
    }

    it should "support interpolation of SingleValues" in {
        IntegerType.interpolate(SingleValue("12"), null).head.asInstanceOf[Int] should be (12)
    }

    it should "support interpolation of ArrayValues" in {
        val result = IntegerType.interpolate(ArrayValue(Array("12","27")), null)
        result.head.asInstanceOf[Int] should be (12)
        result.drop(1).head.asInstanceOf[Int] should be (27)
    }

    it should "support interpolation of Ranges" in {
        val result = IntegerType.interpolate(RangeValue("12","16"), null)
        result.map(_.asInstanceOf[Int]).toSeq should be (Seq(12,13,14,15))
    }

    it should "support interpolation of Ranges with granularity" in {
        val result = IntegerType.interpolate(RangeValue("12","16"), "2")
        result.map(_.asInstanceOf[Int]).toSeq should be (Seq(12,14))
    }
}
