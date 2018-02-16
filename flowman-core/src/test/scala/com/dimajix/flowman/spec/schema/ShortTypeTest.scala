package com.dimajix.flowman.spec.schema

import org.scalatest.FlatSpec
import org.scalatest.Matchers


class ShortTypeTest extends FlatSpec with Matchers {
    "A ShortType" should "parse strings" in {
        ShortType.parse("12").asInstanceOf[Short] should be (12)
    }

    it should "support interpolation of SingleValues" in {
        ShortType.interpolate(SingleValue("12"), null).head.asInstanceOf[Short] should be (12)
    }

    it should "support interpolation of ArrayValues" in {
        val result = ShortType.interpolate(ArrayValue(Array("12","27")), null)
        result.head.asInstanceOf[Short] should be (12)
        result.drop(1).head.asInstanceOf[Short] should be (27)
    }

    it should "support interpolation of Ranges" in {
        val result = ShortType.interpolate(RangeValue("12","16"), null)
        result.map(_.asInstanceOf[Short]).toSeq should be (Seq(12,13,14,15).map(_.toShort))
    }

    it should "support interpolation of Ranges with granularity" in {
        val result = ShortType.interpolate(RangeValue("12","16"), "2")
        result.map(_.asInstanceOf[Short]).toSeq should be (Seq(12,14).map(_.toShort))
    }
}
