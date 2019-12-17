/*
 * Copyright 2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.target

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.VerificationFailedException
import com.dimajix.flowman.spec.ResourceIdentifier
import com.dimajix.flowman.spec.dataset.Dataset
import com.dimajix.flowman.spec.dataset.DatasetSpec
import com.dimajix.flowman.transforms.SchemaEnforcer


case class CompareTarget(
    instanceProperties:Target.Properties,
    actual:Dataset,
    expected:Dataset
) extends BaseTarget {
    private val logger = LoggerFactory.getLogger(classOf[CompareTarget])

    /**
     * Returns all phases which are implemented by this target in the execute method
     * @return
     */
    override def phases : Set[Phase] = Set(Phase.VERIFY)

    /**
     * Returns a list of physical resources required by this target
     *
     * @return
     */
    override def requires(phase: Phase): Set[ResourceIdentifier] = {
        phase match {
            case Phase.VERIFY => actual.requires ++ expected.requires
            case _ => Set()
        }
    }

    /**
      * Performs a verification of the build step or possibly other checks.
      *
      * @param executor
      */
    override protected def verify(executor: Executor): Unit = {
        logger.info(s"Comparing actual dataset '${actual.name}' with expected dataset '${expected.name}'")
        val expectedDf = expected.read(executor, None)
        val actualDf = try {
            actual.read(executor, None)
        }
        catch {
            case ex:Exception => throw new VerificationFailedException(this.identifier, ex)
        }

        // TODO: Compare schemas
        val xfs = SchemaEnforcer(expectedDf.schema)
        val conformedDf = xfs.transform(actualDf)

        val expectedRows = expectedDf.collect().toSeq
        val actualRows = conformedDf.collect().toSeq

        if (prepareAnswer(expectedRows) != prepareAnswer(actualRows)) {
            logger.error(s"Dataset '${actual.name}' does not equal the expected dataset '${expected.name}'")
            logger.error(s"Difference between datasets: \n${genError(expectedRows, actualRows)}")
            throw new VerificationFailedException(identifier)
        }
        else {
            logger.info(s"Dataset '${actual.name}' matches the expected dataset '${expected.name}'")
        }
    }

    private def prepareAnswer(answer: Seq[Row]): Seq[Row] = {
        // Converts data to types that we can do equality comparison using Scala collections.
        // For BigDecimal type, the Scala type has a better definition of equality test (similar to
        // Java's java.math.BigDecimal.compareTo).
        // For binary arrays, we convert it to Seq to avoid of calling java.util.Arrays.equals for
        // equality test.
        answer.map(prepareRow).sortBy(_.toString())
    }

    // We need to call prepareRow recursively to handle schemas with struct types.
    private def prepareRow(row: Row): Row = {
        Row.fromSeq(row.toSeq.map {
            case null => null
            case d: java.math.BigDecimal => BigDecimal(d)
            // Convert array to Seq for easy equality checkJob.
            case b: Array[_] => b.toSeq
            case r: Row => prepareRow(r)
            case o => o
        })
    }

    private def sideBySide(left: Seq[String], right: Seq[String]): Seq[String] = {
        val maxLeftSize = left.map(_.length).max
        val leftPadded = left ++ Seq.fill(math.max(right.size - left.size, 0))("")
        val rightPadded = right ++ Seq.fill(math.max(left.size - right.size, 0))("")

        leftPadded.zip(rightPadded).map {
            case (l, r) => (if (l == r) " " else "!") + l + (" " * ((maxLeftSize - l.length) + 3)) + r
        }
    }

    private def genError(expectedAnswer: Seq[Row],
                         sparkAnswer: Seq[Row]): String = {
        val getRowType: Option[Row] => String = row =>
            row.map(row =>
                if (row.schema == null) {
                    "struct<>"
                } else {
                    s"${row.schema.catalogString}"
                }).getOrElse("struct<>")

        s"""
           |== Results ==
           |${
            sideBySide(
                s"== Expected - ${expectedAnswer.size} ==" +:
                    getRowType(expectedAnswer.headOption) +:
                    prepareAnswer(expectedAnswer).map(_.toString()),
                s"== Actual - ${sparkAnswer.size} ==" +:
                    getRowType(sparkAnswer.headOption) +:
                    prepareAnswer(sparkAnswer).map(_.toString())).mkString("\n")
        }
    """.stripMargin
    }
}


class CompareTargetSpec extends TargetSpec {
    @JsonProperty(value = "actual", required = true) private var actual: DatasetSpec = _
    @JsonProperty(value = "expected", required = true) private var expected: DatasetSpec = _

    override def instantiate(context: Context): CompareTarget = {
        CompareTarget(
            instanceProperties(context),
            actual.instantiate(context),
            expected.instantiate(context)
        )
    }
}
