package com.dimajix.flowman.sources.spark

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.LocalSparkSession


class SequenceFileFormatTest extends FlatSpec with Matchers with LocalSparkSession {
    "A SequenceFile" should "be readble" in {
        val df = spark.read
            .format("sequencefile")
            .load("lala")
        df should not be (null)
    }
}
