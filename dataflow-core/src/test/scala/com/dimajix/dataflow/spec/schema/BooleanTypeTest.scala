package com.dimajix.dataflow.spec.schema

import org.scalatest.FlatSpec
import org.scalatest.Matchers


class BooleanTypeTest extends FlatSpec with Matchers {
    "A BooleanType" should "parse strings" in {
        BooleanType.parse("true").asInstanceOf[Boolean] should be (true)
        BooleanType.parse("false").asInstanceOf[Boolean] should be (false)
    }

    it should "support interpolation of SingleValues" in {
        BooleanType.interpolate(SingleValue("true"), null).head.asInstanceOf[Boolean] should be (true)
    }

    it should "support interpolation of ArrayValues" in {
        val result = BooleanType.interpolate(ArrayValue(Array("true","false")), null)
        result.head.asInstanceOf[Boolean] should be (true)
        result.drop(1).head.asInstanceOf[Boolean] should be (false)
    }
}
