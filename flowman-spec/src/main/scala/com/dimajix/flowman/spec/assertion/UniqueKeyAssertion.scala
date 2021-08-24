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
import org.apache.spark.sql.functions
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.Assertion
import com.dimajix.flowman.model.AssertionResult
import com.dimajix.flowman.model.AssertionTestResult
import com.dimajix.flowman.model.BaseAssertion
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.spark.sql.DataFrameUtils


case class UniqueKeyAssertion(
    override val instanceProperties:Assertion.Properties,
    mapping: MappingOutputIdentifier,
    key: Seq[String]
) extends BaseAssertion {
    private val logger = LoggerFactory.getLogger(classOf[UniqueKeyAssertion])

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
    override def inputs: Seq[MappingOutputIdentifier] = Seq(mapping)

    /**
     * Executes this [[Assertion]] and returns a corresponding DataFrame
     *
     * @param execution
     * @param input
     * @return
     */
    override def execute(execution: Execution, input: Map[MappingOutputIdentifier, DataFrame]): Seq[AssertionTestResult] = {
        require(execution != null)
        require(input != null)

        val name = s"unique_key for '$mapping' with keys '${key.mkString(",")}'"
        val df = input(mapping)
        val duplicates = df.groupBy(key.map(df.apply):_*)
            .agg(functions.count("*").as("flowman_key_count"))
            .filter("flowman_key_count > 1")
        val numDuplicates = duplicates.count()

        val status = if (numDuplicates > 0) {
            val diff = DataFrameUtils.showString(duplicates, 20, -1)
            logger.error(s"""Mapping '$mapping' contains $numDuplicates duplicate entries for key '${key.mkString(",")}':\n$diff""")
            false
        }
        else {
            true
        }

        Seq(AssertionTestResult(name, status))
    }
}


class UniqueKeyAssertionSpec extends AssertionSpec {
    @JsonProperty(value="mapping", required=true) private var mapping:String = _
    @JsonProperty(value="key", required=true) private var key:Seq[String] = Seq()

    override def instantiate(context: Context): UniqueKeyAssertion = {
        UniqueKeyAssertion(
            instanceProperties(context),
            MappingOutputIdentifier.parse(context.evaluate(mapping)),
            key.map(context.evaluate)
        )
    }
}
