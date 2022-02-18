/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.documentation

import scala.util.control.NonFatal

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import com.dimajix.common.ExceptionUtils.reasons
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.spi.ColumnTestExecutor


class TestExecutor(execution: Execution) {
    private val logger = LoggerFactory.getLogger(getClass)
    private val columnTestExecutors = ColumnTestExecutor.executors

    /**
     * Executes all tests for a relation as defined within the documentation
     * @param relation
     * @param doc
     * @return
     */
    def executeTests(relation:Relation, doc:RelationDoc) : RelationDoc = {
        val schemaDoc = doc.schema.map { schema =>
            if (containsTests(schema)) {
                logger.info(s"Conducting tests on relation '${relation.identifier}'")
                try {
                    val df = relation.read(execution, doc.partitions)
                    runSchemaTests(relation.context, df, schema)
                } catch {
                    case NonFatal(ex) =>
                        logger.warn(s"Error executing tests for relation '${relation.identifier}': ${reasons(ex)}")
                        failSchemaTests(schema)
                }
            }
            else {
                schema
            }
        }
        doc.copy(schema=schemaDoc)
    }

    /**
     * Executes all tests for a mapping as defined within the documentation
     * @param relation
     * @param doc
     * @return
     */
    def executeTests(mapping:Mapping, doc:MappingDoc) : MappingDoc = {
        val outputs = doc.outputs.map { output =>
            val schema = output.schema.map { schema =>
                if (containsTests(schema)) {
                    logger.info(s"Conducting tests on mapping '${mapping.identifier}'")
                    try {
                        val df = execution.instantiate(mapping, output.name)
                        runSchemaTests(mapping.context, df, schema)
                    } catch {
                        case NonFatal(ex) =>
                            logger.warn(s"Error executing tests for mapping '${mapping.identifier}': ${reasons(ex)}")
                            failSchemaTests(schema)
                    }
                }
                else {
                    schema
                }
            }
            output.copy(schema=schema)
        }
        doc.copy(outputs=outputs)
    }

    private def containsTests(doc:SchemaDoc) : Boolean = {
        doc.tests.nonEmpty || containsTests(doc.columns)
    }
    private def containsTests(docs:Seq[ColumnDoc]) : Boolean = {
        docs.exists(col => col.tests.nonEmpty || containsTests(col.children))
    }

    private def failSchemaTests(schema:SchemaDoc) : SchemaDoc = {
        val columns = failColumnTests(schema.columns)
        schema.copy(columns=columns)
    }
    private def failColumnTests(columns:Seq[ColumnDoc]) : Seq[ColumnDoc] = {
        columns.map(col => failColumnTests(col))
    }
    private def failColumnTests(column:ColumnDoc) : ColumnDoc = {
        val tests = column.tests.map { test =>
            val result = TestResult(Some(test.reference), status = TestStatus.ERROR)
            test.withResult(result)
        }
        val children = failColumnTests(column.children)
        column.copy(children=children, tests=tests)
    }

    private def runSchemaTests(context:Context, df:DataFrame, schema:SchemaDoc) : SchemaDoc = {
        val columns = runColumnTests(context, df, schema.columns)
        schema.copy(columns=columns)
    }
    private def runColumnTests(context:Context, df:DataFrame, columns:Seq[ColumnDoc], path:String = "") : Seq[ColumnDoc] = {
        columns.map(col => runColumnTests(context, df, col, path))
    }
    private def runColumnTests(context:Context, df:DataFrame, column:ColumnDoc, path:String) : ColumnDoc = {
        val columnPath = path + column.name
        val tests = column.tests.map { test =>
            val result =
                try {
                    val result = columnTestExecutors.flatMap(_.execute(execution, context, df, columnPath, test)).headOption
                    result match {
                        case None =>
                            logger.warn(s"Could not find appropriate test executor for testing column $columnPath")
                            TestResult(Some(test.reference), status = TestStatus.NOT_RUN)
                        case Some(result) =>
                            result.reparent(test.reference)
                    }
                } catch {
                    case NonFatal(ex) =>
                        logger.warn(s"Error executing column test: ${reasons(ex)}")
                        TestResult(Some(test.reference), status = TestStatus.ERROR)

                }
            test.withResult(result)
        }
        val children = runColumnTests(context, df, column.children, path + column.name + ".")
        column.copy(children=children, tests=tests)
    }
}
