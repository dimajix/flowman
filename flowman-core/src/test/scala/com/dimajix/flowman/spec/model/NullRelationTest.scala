package com.dimajix.flowman.spec.model

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.LocalSparkSession
import com.dimajix.flowman.execution.Session


class NullRelationTest extends FlatSpec with Matchers with LocalSparkSession {
    "The NullRelation" should "provide an empty DataFrame" in {
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        val relation = new NullRelation
        val schema = StructType(
            StructField("lala", StringType) :: Nil
        )
        val df = relation.read(executor, schema)
        df should not be (null)
    }
}
