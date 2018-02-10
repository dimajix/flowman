package com.dimajix.dataflow.spec.schema

import org.scalatest.FlatSpec
import org.scalatest.Matchers


class ByteTypeTest extends FlatSpec with Matchers {
    "A ByteType" should "parse strings" in {
        ByteType.parse("12").asInstanceOf[Byte] should be (12)
    }

    it should "support interpolation of SingleValues" in {
        ByteType.interpolate(SingleValue("12"), null).head.asInstanceOf[Byte] should be (12)
    }

    it should "support interpolation of ArrayValues" in {
        val result = ByteType.interpolate(ArrayValue(Array("12","27")), null)
        result.head.asInstanceOf[Byte] should be (12)
        result.drop(1).head.asInstanceOf[Byte] should be (27)
    }

    it should "support interpolation of Ranges" in {
        val result = ByteType.interpolate(RangeValue("12","16"), null)
        result.map(_.asInstanceOf[Byte]).toSeq should be (Seq(12,13,14,15).map(_.toByte))
    }

    it should "support interpolation of Ranges with granularity" in {
        val result = ByteType.interpolate(RangeValue("12","16"), "2")
        result.map(_.asInstanceOf[Byte]).toSeq should be (Seq(12,14).map(_.toByte))
    }
}
