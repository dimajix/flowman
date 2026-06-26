/*
 * Copyright (C) 2022 The Flowman Authors
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
import com.dimajix.common.text.ConsoleColors.yellow
import com.dimajix.flowman.common.ThreadUtils
import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.spi.ColumnCheckExecutor
import com.dimajix.flowman.spi.SchemaCheckExecutor
import com.dimajix.spark.SparkUtils
import com.dimajix.spark.SparkUtils.withJobGroup


class CheckExecutor(execution: Execution) {
    private val logger = LoggerFactory.getLogger(getClass)
    private val columnTestExecutors = ColumnCheckExecutor.executors
    private val schemaTestExecutors = SchemaCheckExecutor.executors

    /**
     * Executes all checks for a relation as defined within the documentation
     * @param relation
     * @param doc
     * @return
     */
    def executeTests(relation:Relation, doc:RelationDoc) : RelationDoc = {
        val schemaDoc = doc.schema.map { schema =>
            if (containsChecks(schema)) {
                logger.info(s"Conducting checks on relation '${relation.identifier}'")
                try {
                    val df = relation.read(execution, doc.partitions)
                    runAllChecks(relation.context, df, schema)
                } catch {
                    case NonFatal(ex) =>
                        logger.warn(yellow(s"Error executing checks for relation '${relation.identifier}':\n  ${reasons(ex)}"))
                        failAllChecks(schema)
                }
            }
            else {
                schema
            }
        }
        doc.copy(schema=schemaDoc)
    }

    /**
     * Executes all checks for a mapping as defined within the documentation
     * @param relation
     * @param doc
     * @return
     */
    def executeTests(mapping:Mapping, doc:MappingDoc) : MappingDoc = {
        val outputs = doc.outputs.map { output =>
            val schema = output.schema.map { schema =>
                if (containsChecks(schema)) {
                    logger.info(s"Conducting checks on mapping '${mapping.identifier}'")
                    try {
                        val df = execution.instantiate(mapping, output.name)
                        runAllChecks(mapping.context, df, schema)
                    } catch {
                        case NonFatal(ex) =>
                            logger.warn(yellow(s"Error executing checks for mapping '${mapping.identifier}':\n  ${reasons(ex)}"))
                            failAllChecks(schema)
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

    private def containsChecks(doc:SchemaDoc) : Boolean = {
        doc.checks.nonEmpty || containsChecks(doc.columns)
    }
    private def containsChecks(docs:Seq[ColumnDoc]) : Boolean = {
        docs.exists(col => col.checks.nonEmpty || containsChecks(col.children))
    }

    private def failAllChecks(schema:SchemaDoc) : SchemaDoc = {
        val columns = failColumnChecks(schema.columns)
        val tests = schema.checks.map { test =>
            val result = CheckResult(Some(test.reference), status = CheckStatus.ERROR)
            test.withResult(result)
        }
        schema.copy(columns=columns, checks=tests)
    }
    private def failColumnChecks(columns:Seq[ColumnDoc]) : Seq[ColumnDoc] = {
        columns.map(col => failColumnChecks(col))
    }
    private def failColumnChecks(column:ColumnDoc) : ColumnDoc = {
        val tests = column.checks.map { test =>
            val result = CheckResult(Some(test.reference), status = CheckStatus.ERROR)
            test.withResult(result)
        }
        val children = failColumnChecks(column.children)
        column.copy(children=children, checks=tests)
    }

    private def runAllChecks(context:Context, df:DataFrame, schema:SchemaDoc) : SchemaDoc = {
        val parallelism = context.flowmanConf.getConf(FlowmanConf.EXECUTION_CHECK_PARALLELISM)
        // Get current Spark JobGroup and JobDescription to pass to other threads
        implicit val jobGroup: (String, String) = SparkUtils.getJobGroup(execution.spark.sparkContext)

        try {
            val columnChecks = runColumnChecks(context, df, schema.columns, parallelism)
            val schemaChecks = runSchemaChecks(context, df, schema, parallelism)
            schemaChecks.copy(columns = columnChecks)
        } catch {
            case NonFatal(ex) =>
                logger.warn(yellow(s"Error executing checks:\n  ${reasons(ex)}"))
                failAllChecks(schema)
        }
    }

    private def runSchemaChecks(context:Context, df:DataFrame, schema:SchemaDoc, parallelism:Int)(implicit jobGroup:(String,String)) : SchemaDoc = {
        val tests = ThreadUtils.parmap(schema.checks, "schema-checks", parallelism) { test =>
            withJobGroup(df.sparkSession.sparkContext, jobGroup._1, jobGroup._2) {
                runSchemaCheck(context, df, test)
            }
        }
        schema.copy(checks = tests)
    }
    private def runSchemaCheck(context: Context, df: DataFrame, test: SchemaCheck) : SchemaCheck = {
        logger.info(s" - Executing schema test '${test.name}'")
        val result =
            try {
                val result = schemaTestExecutors.flatMap(_.execute(execution, context, df, test)).headOption
                result match {
                    case None =>
                        logger.warn(yellow(s"Could not find appropriate test executor for testing schema"))
                        CheckResult(Some(test.reference), status = CheckStatus.NOT_RUN)
                    case Some(result) =>
                        result.reparent(test.reference)
                }
            } catch {
                case NonFatal(ex) =>
                    logger.warn(yellow(s"Error executing schema test:\n  ${reasons(ex)}"))
                    CheckResult(Some(test.reference), status = CheckStatus.ERROR)

            }
        test.withResult(result)
    }

    private def runColumnChecks(context:Context, df:DataFrame, columns:Seq[ColumnDoc], parallelism:Int, path:String = "")(implicit jobGroup:(String,String)) : Seq[ColumnDoc] = {
        ThreadUtils.parmap(columns, "column-checks", parallelism) { col =>
            runColumnChecks(context, df, col, parallelism, path)
        }
    }
    private def runColumnChecks(context:Context, df:DataFrame, column:ColumnDoc, parallelism:Int, path:String)(implicit jobGroup:(String,String)) : ColumnDoc = {
        val columnPath = path + column.name
        val tests = ThreadUtils.parmap(column.checks, "column-tests", parallelism) { test =>
            withJobGroup(df.sparkSession.sparkContext, jobGroup._1, jobGroup._2) {
                runColumnCheck(context, df, test, columnPath)
            }
        }

        val children = runColumnChecks(context, df, column.children, parallelism, path + column.name + ".")
        column.copy(children=children, checks=tests)
    }
    private def runColumnCheck(context:Context, df:DataFrame, test:ColumnCheck, columnPath:String) : ColumnCheck = {
        logger.info(s" - Executing test '${test.name}' on column ${columnPath}")
        val result =
            try {
                val result = columnTestExecutors.flatMap(_.execute(execution, context, df, columnPath, test)).headOption
                result match {
                    case None =>
                        logger.warn(yellow(s"Could not find appropriate test executor for testing column $columnPath"))
                        CheckResult(Some(test.reference), status = CheckStatus.NOT_RUN)
                    case Some(result) =>
                        result.reparent(test.reference)
                }
            } catch {
                case NonFatal(ex) =>
                    logger.warn(yellow(s"Error executing column test:\n  ${reasons(ex)}"))
                    CheckResult(Some(test.reference), status = CheckStatus.ERROR)

            }
        test.withResult(result)
    }
}
