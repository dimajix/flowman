package org.apache.spark.sql

import java.sql.Timestamp

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.spark.testing.LocalSparkSession
import com.dimajix.util.DateTimeUtils


class DateTimeTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    def localTime(str:String) : Timestamp = new Timestamp(DateTimeUtils.stringToTime(str).getTime)

    "Date truncation" should "work in" in {
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        val df = spark.createDataFrame(Seq((localTime("2019-02-01T12:34:03"), "")))

        val result = df.selectExpr("date_trunc('day', _1)")
        val collectedRows = result.collect()
        result.show()
    }
}
