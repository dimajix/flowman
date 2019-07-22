package com.dimajix.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.spark.functions._
import com.dimajix.spark.testing.LocalSparkSession


object NullableStructTest {
    case class Person(name:String, age:Option[Int])
}

class NullableStructTest extends FlatSpec with Matchers with LocalSparkSession {
    import NullableStructTest.Person

    "The nullable_struct function" should "return non-null values" in {
        val df = spark.createDataFrame(Seq(
            Person("Bob", Some(23)),
            Person("Alice", None),
            Person(null, None)
        ))

        val result = df.select(nullable_struct(df("name"), df("age")) alias "s")
        val rows = result.orderBy(col("s.name"))
            .collect()
        rows.toSeq should be (Seq(
            Row(null),
            Row(Row("Alice", null)),
            Row(Row("Bob", 23))
        ))
    }

    it should "work with unresolved columns" in {
        val df = spark.createDataFrame(Seq(
            Person("Bob", Some(23)),
            Person("Alice", None),
            Person(null, None)
        ))

        val result = df.repartition(2).select(nullable_struct(col("name"), col("age")) alias "s")
        //result.queryExecution.debug.codegen()
        val rows = result.orderBy(col("s.name"))
            .collect()
        rows.toSeq should be (Seq(
            Row(null),
            Row(Row("Alice", null)),
            Row(Row("Bob", 23))
        ))
    }
}
