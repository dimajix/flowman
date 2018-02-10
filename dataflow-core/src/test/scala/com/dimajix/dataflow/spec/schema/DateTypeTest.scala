package com.dimajix.dataflow.spec.schema

import java.sql.Date

import org.scalatest.FlatSpec
import org.scalatest.Matchers


class DateTypeTest extends FlatSpec with Matchers {
    "A DateType" should "parse strings" in {
        DateType.parse("2017-12-01").asInstanceOf[Date] should be (Date.valueOf("2017-12-01"))
    }

    it should "support interpolation of SingleValues" in {
        DateType.interpolate(SingleValue("2017-12-20"), null).head.asInstanceOf[Date] should be (Date.valueOf("2017-12-20"))
    }

    it should "support interpolation of ArrayValues" in {
        val result = DateType.interpolate(ArrayValue(Array("2017-12-10","2017-12-14")), null).toSeq
        result.size should be (2)
        result(0).asInstanceOf[Date] should be (Date.valueOf("2017-12-10"))
        result(1).asInstanceOf[Date] should be (Date.valueOf("2017-12-14"))
    }

    it should "support interpolation of Ranges" in {
        val result = DateType.interpolate(RangeValue("2017-12-10","2017-12-14"), null).toSeq
        result.size should be (4)
        result(0).asInstanceOf[Date] should be (Date.valueOf("2017-12-10"))
        result(1).asInstanceOf[Date] should be (Date.valueOf("2017-12-11"))
        result(3).asInstanceOf[Date] should be (Date.valueOf("2017-12-13"))
    }

    it should "support interpolation of Ranges with granularity" in {
        val result = DateType.interpolate(RangeValue("2017-12-10","2017-12-18"), "P2D").toSeq
        result.size should be (4)
        result(0).asInstanceOf[Date] should be (Date.valueOf("2017-12-10"))
        result(1).asInstanceOf[Date] should be (Date.valueOf("2017-12-12"))
        result(3).asInstanceOf[Date] should be (Date.valueOf("2017-12-16"))
    }
}
