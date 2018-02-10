package com.dimajix.dataflow.spec.schema

import org.scalatest.FlatSpec
import org.scalatest.Matchers


class LongTypeTest extends FlatSpec with Matchers {
    "A LongType" should "parse strings" in {
        LongType.parse("12").asInstanceOf[Long] should be (12)
    }

    it should "support interpolation of SingleValues" in {
        LongType.interpolate(SingleValue("12"), null).head.asInstanceOf[Long] should be (12)
    }

    it should "support interpolation of ArrayValues" in {
        val result = LongType.interpolate(ArrayValue(Array("12","27")), null)
        result.head.asInstanceOf[Long] should be (12)
        result.drop(1).head.asInstanceOf[Long] should be (27)
    }

    it should "support interpolation of Ranges" in {
        val result = LongType.interpolate(RangeValue("12","16"), null)
        result.map(_.asInstanceOf[Long]).toSeq should be (Seq(12,13,14,15).map(_.toLong))
    }

    it should "support interpolation of Ranges with granularity" in {
        val result = LongType.interpolate(RangeValue("12","16"), "2")
        result.map(_.asInstanceOf[Long]).toSeq should be (Seq(12,14).map(_.toLong))
    }
}
