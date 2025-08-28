/*
 * Copyright (C) 2019-2025 The Flowman Authors
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
import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.ExecutionException
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.VerificationFailedException
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.Dataset
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.spec.dataset.DatasetSpec
import com.dimajix.flowman.transforms.SchemaEnforcer
import com.dimajix.spark.sql.DataFrameUtils
import com.dimajix.spark.sql.SchemaUtils


final case class CompareTarget(
    instanceProperties:Target.Properties,
    actual:Dataset,
    expected:Dataset,
    compareSchema:Boolean=false,
    ignoreTypes:Boolean = false,
    ignoreNullability:Boolean = true,
    ignoreCase:Boolean = false,
    ignoreOrder:Boolean = false
) extends BaseTarget {
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
     * Returns the state of the target, specifically of any artifacts produces. If this method return [[Yes]],
     * then an [[execute]] should update the output, such that the target is not 'dirty' any more.
     *
     * @param execution
     * @param phase
     * @return
     */
    override def dirty(execution: Execution, phase: Phase): Trilean = {
        phase match {
            case Phase.VERIFY => Yes
            case _ => No
        }
    }

    /**
      * Performs a verification of the build step or possibly other checks.
      *
      * @param execution
      */
    override protected def verify(execution: Execution): Unit = {
        val logger = getLogger(execution)

        logger.info(s"Comparing actual dataset '${actual.name}' with expected dataset '${expected.name}'")
        val expectedDf = expected.read(execution)
        val actualDf = try {
            actual.read(execution)
        }
        catch {
            case ex:Exception => throw new VerificationFailedException(this.identifier, ex)
        }

        // First: Compare schemas
        if (compareSchema) {
            val actualSchema = actualDf.schema
            val expectedSchema = expectedDf.schema
            if (!SchemaUtils.compare(expectedSchema, actualSchema, ignoreTypes, ignoreNullability, ignoreOrder, ignoreCase)) {
                logger.error(s"Dataset '${actual.name}' has different schema than the expected dataset '${expected.name}'.\nActual schema:\n${actualSchema.treeString}\nExpected schema:\n${expectedSchema.treeString}")
                throw new VerificationFailedException(identifier, new ExecutionException(s"Dataset '${actual.name}' has different schema than the expected dataset '${expected.name}'"))
            }
        }

        // If schemas match, then compare records
        val xfs = SchemaEnforcer(expectedDf.schema)
        val conformedDf = xfs.transform(actualDf)

        val diff = DataFrameUtils.diff(expectedDf, conformedDf)
        if (diff.nonEmpty) {
            logger.error(s"Dataset '${actual.name}' does not equal the expected dataset '${expected.name}'")
            logger.error(s"Difference between datasets: \n${diff.get}")
            throw new VerificationFailedException(identifier, new ExecutionException(s"Dataset '${actual.name}' does not equal the expected dataset '${expected.name}'"))
        }
        else {
            logger.info(s"Dataset '${actual.name}' matches the expected dataset '${expected.name}'")
        }
    }
}


class CompareTargetSpec extends TargetSpec {
    @JsonProperty(value = "actual", required = true) private var actual: DatasetSpec = _
    @JsonProperty(value = "expected", required = true) private var expected: DatasetSpec = _
    @JsonProperty(value = "compareSchema", required = false) private var compareSchema: String = "false"
    @JsonProperty(value = "ignoreTypes", required = false) private var ignoreTypes: String = "false"
    @JsonProperty(value = "ignoreNullability", required = false) private var ignoreNullability: String = "false"
    @JsonProperty(value = "ignoreCase", required = false) private var ignoreCase: String = "false"
    @JsonProperty(value = "ignoreOrder", required = false) private var ignoreOrder: String = "false"

    override def instantiate(context: Context, properties:Option[Target.Properties] = None): CompareTarget = {
        CompareTarget(
            instanceProperties(context, properties),
            actual.instantiate(context),
            expected.instantiate(context),
            context.evaluate(compareSchema).toBoolean,
            context.evaluate(ignoreTypes).toBoolean,
            context.evaluate(ignoreNullability).toBoolean,
            context.evaluate(ignoreCase).toBoolean,
            context.evaluate(ignoreOrder).toBoolean
        )
    }
}
