/*
 * Copyright 2021 Kaya Kupferschmidt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dimajix.flowman.spec.assertion

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.Assertion
import com.dimajix.flowman.model.AssertionResult
import com.dimajix.flowman.model.BaseAssertion
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.spark.sql.DataFrameUtils
import com.dimajix.spark.sql.SqlParser


object SqlAssertion {
    case class Case(
        query:String,
        expected:Seq[Array[String]] = Seq()
    ) {
        def sql : String = query

        override def hashCode(): Int = {
            (query, expected.map(_.toSeq)).hashCode()
        }

        override def equals(obj: Any): Boolean = {
            if (obj == null) {
                false
            }
            else if (super.equals(obj)) {
                true
            }
            else if (!obj.isInstanceOf[Case]) {
                false
            }
            else {
                val otherCase = obj.asInstanceOf[Case]
                val l = (query, expected.map(_.toSeq))
                val r = (otherCase.query, otherCase.expected.map(_.toSeq))
                l == r
            }
        }
    }
}
case class SqlAssertion(
    override val instanceProperties:Assertion.Properties,
    tests: Seq[SqlAssertion.Case]
) extends BaseAssertion {
    private val logger = LoggerFactory.getLogger(classOf[SqlAssertion])

    /**
     * Returns a list of physical resources required by this assertion. This list will only be non-empty for assertions
     * which actually read from physical data.
     *
     * @return
     */
    override def requires: Set[ResourceIdentifier] = Set()

    /**
     * Returns the dependencies (i.e. names of tables in the Dataflow model)
     *
     * @return
     */
    override def inputs: Seq[MappingOutputIdentifier] = {
        tests.flatMap(test => SqlParser.resolveDependencies(test.sql))
            .map(MappingOutputIdentifier.parse)
            .distinct
    }

    /**
     * Executes this [[Assertion]] and returns a corresponding DataFrame
     *
     * @param execution
     * @param input
     * @return
     */
    override def execute(execution: Execution, input: Map[MappingOutputIdentifier, DataFrame]): Seq[AssertionResult] = {
        require(execution != null)
        require(input != null)

        DataFrameUtils.withTempViews(input.map(kv => kv._1.name -> kv._2)) {
            tests.par.map { test =>
                // Execute query
                val sql = test.sql
                val actual = execution.spark.sql(sql)

                val result = DataFrameUtils.diffToStringValues(test.expected, actual)
                result match {
                    case Some(diff) =>
                        logger.error(s"failed query: $sql\n$diff")
                        AssertionResult(sql, false)
                    case None =>
                        AssertionResult(sql, true)
                }
            }.toList
        }
    }
}


object SqlAssertionSpec {
    class Case {
        @JsonProperty(value="query", required=true) private var query:String = ""
        @JsonProperty(value="expected", required=true) private var expected:Seq[Array[String]] = Seq()

        def instantiate(context:Context) : SqlAssertion.Case = {
            SqlAssertion.Case(
                context.evaluate(query),
                expected.map(_.map(context.evaluate))
            )
        }
    }
}
class SqlAssertionSpec extends AssertionSpec {
    @JsonProperty(value="tests", required=false) private var tests:Seq[SqlAssertionSpec.Case] = Seq()
    @JsonProperty(value="query", required=false) private var query:String = ""
    @JsonProperty(value="expected", required=false) private var expected:Seq[Array[String]] = Seq()

    override def instantiate(context: Context): SqlAssertion = {
        val embeddedQuery = context.evaluate(query)
        val embeddedExpectation = expected.map(_.map(context.evaluate))
        val embeddedCase = if (embeddedQuery.nonEmpty) Some(SqlAssertion.Case(embeddedQuery, embeddedExpectation)) else None

        SqlAssertion(
            instanceProperties(context),
            embeddedCase.toSeq ++ tests.map(_.instantiate(context))
        )
    }
}
