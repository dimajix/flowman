package com.dimajix.flowman.sources.local

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.sources.local.implicits._
import com.dimajix.flowman.LocalSparkSession


class DataFrameReaderTest extends FlatSpec with Matchers with LocalSparkSession {
    "The DataFrameReader" should "be instantiated by a readLocal call" in {
        spark.readLocal should not be (null)
    }
}
