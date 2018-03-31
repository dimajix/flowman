package com.dimajix.flowman.sources.local

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.sources.local.implicits._
import com.dimajix.flowman.LocalSparkSession

class DataFrameWriterTest extends FlatSpec with Matchers with LocalSparkSession {
    "The DataFrameWriter" should "be instantiated by a readLocal call" in {
        val writer = spark.emptyDataFrame.writeLocal
        writer should not be (null)
        writer.mode("overwrite")
        writer.mode("append")
        writer.mode("ignore")
        writer.mode("error")
    }

}
