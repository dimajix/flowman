package com.dimajix.flowman

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.scalatest.FlatSpec
import org.scalatest.Matchers

/**
  * Created by kaya on 22.09.16.
  */
class SqlExpressionTest extends FlatSpec with Matchers {
    "The SqlParser" should "parse simple columns" in {
        val parser = CatalystSqlParser
        val result = parser.parseExpression("some_column")
        result.references.map(_.name).toSeq.contains("some_column") should be (true)
    }
    it should "parse complex expressions" in {
        val parser = CatalystSqlParser
        val result = parser.parseExpression("case when some_column > 0 then some_udf(other_column) else max(third_column) end")
        result.references.map(_.name).toSeq.contains("some_column") should be (true)
        result.references.map(_.name).toSeq.contains("other_column") should be (true)
        result.references.map(_.name).toSeq.contains("third_column") should be (true)
    }
}
