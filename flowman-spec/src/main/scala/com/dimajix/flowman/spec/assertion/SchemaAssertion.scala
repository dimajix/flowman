/*
 * Copyright (C) 2021-2025 The Flowman Authors
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

import com.dimajix.flowman.execution.{Context, Execution}
import com.dimajix.flowman.model._
import com.dimajix.flowman.spec.schema.SchemaSpec
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.{types => ftypes}
import com.dimajix.jackson.ListMapDeserializer
import com.dimajix.spark.sql.SchemaUtils
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import scala.collection.immutable.ListMap


case class SchemaAssertion(
    override val instanceProperties:Assertion.Properties,
    mapping: MappingOutputIdentifier,
    columns:Seq[Field] = Seq(),
    schema:Option[Schema] = None,
    ignoreTypes: Boolean = false,
    ignoreNullability: Boolean = true,
    ignoreCase: Boolean = false,
    ignoreOrder: Boolean = false
                          ) extends BaseAssertion {
    private val logger = LoggerFactory.getLogger(classOf[SchemaAssertion])

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
    override def inputs: Set[MappingOutputIdentifier] = Set(mapping)

    /**
     * Executes this [[Assertion]] and returns a corresponding DataFrame
     *
     * @param execution
     * @param input
     * @return
     */
    override def execute(execution: Execution, input: Map[MappingOutputIdentifier, DataFrame]): AssertionResult = {
        require(execution != null)
        require(input != null)

        AssertionResult.of(this) {
            val actualSchema = input(mapping).schema
            val desiredSchema = schema.map(_.sparkSchema).getOrElse(ftypes.StructType(columns).sparkType)

            Seq(AssertionTestResult.of(s"schema for '$mapping'") {
                if (!SchemaUtils.compare(actualSchema, desiredSchema, ignoreTypes, ignoreNullability, ignoreOrder, ignoreCase)) {
                    logger.error(s"""Mapping '$mapping' has wrong schema.\nActual schema:\n${actualSchema.treeString}\nExpected schema:\n${desiredSchema.treeString}""")
                    false
                }
                else {
                    true
                }
            })
        }
    }
}


class SchemaAssertionSpec extends AssertionSpec {
    @JsonProperty(value="mapping", required=true) private var mapping:String = _
    @JsonDeserialize(using = classOf[ListMapDeserializer]) // Old Jackson in old Spark doesn't support ListMap
    @JsonProperty(value = "columns", required = false) private var columns: ListMap[String,String] = ListMap()
    @JsonProperty(value = "schema", required = false) private var schema: Option[SchemaSpec] = None
    @JsonProperty(value = "ignoreTypes", required = false) private var ignoreTypes: String = "false"
    @JsonProperty(value = "ignoreNullability", required = false) private var ignoreNullability: String = "false"
    @JsonProperty(value = "ignoreCase", required = false) private var ignoreCase: String = "false"
    @JsonProperty(value = "ignoreOrder", required = false) private var ignoreOrder: String = "false"

    override def instantiate(context: Context, properties:Option[Assertion.Properties] = None): SchemaAssertion = {
        SchemaAssertion(
            instanceProperties(context, properties),
            MappingOutputIdentifier.parse(context.evaluate(mapping)),
            columns.toSeq.map(kv => Field(kv._1, ftypes.FieldType.of(context.evaluate(kv._2)))),
            schema.map(_.instantiate(context)),
            context.evaluate(ignoreTypes).toBoolean,
            context.evaluate(ignoreNullability).toBoolean,
            context.evaluate(ignoreCase).toBoolean,
            context.evaluate(ignoreOrder).toBoolean
        )
    }
}
