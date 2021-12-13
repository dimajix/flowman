/*
 * Copyright 2018 Kaya Kupferschmidt
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
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.Dataset
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.spec.dataset.DatasetSpec
import com.dimajix.flowman.spec.dataset.MappingDataset
import com.dimajix.flowman.spec.dataset.RelationDataset
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.util.ConsoleUtils


object ConsoleTarget {
    def apply(context: Context, dataset: Dataset, limit:Int, columns:Seq[String], header:Boolean, csv:Boolean) : ConsoleTarget = {
        new ConsoleTarget(
            Target.Properties(context),
            dataset,
            limit,
            header,
            csv,
            columns
        )
    }
    def apply(context: Context, output: MappingOutputIdentifier, limit:Int, columns:Seq[String], header:Boolean, csv:Boolean) : ConsoleTarget = {
        new ConsoleTarget(
            Target.Properties(context),
            MappingDataset(context, output),
            limit,
            header,
            csv,
            columns
        )
    }
    def apply(context: Context, relation: RelationIdentifier, limit:Int, columns:Seq[String], partitions:Map[String,SingleValue]=Map(), header:Boolean, csv:Boolean) : ConsoleTarget = {
        new ConsoleTarget(
            Target.Properties(context),
            RelationDataset(context, relation, partitions),
            limit,
            header,
            csv,
            columns
        )
    }
}
case class ConsoleTarget(
    instanceProperties:Target.Properties,
    dataset:Dataset,
    limit:Int,
    header:Boolean,
    csv:Boolean,
    columns:Seq[String]
) extends BaseTarget {
    /**
     * Returns all phases which are implemented by this target in the execute method
     * @return
     */
    override def phases : Set[Phase] = Set(Phase.BUILD)

    /**
      * Returns a list of physical resources required by this target
      * @return
      */
    override def requires(phase: Phase) : Set[ResourceIdentifier] = {
        phase match {
            case Phase.BUILD => dataset.requires
            case _ => Set()
        }
    }

    /**
     * Returns the state of the target, specifically of any artifacts produces. If this method return [[Yes]],
     * then an [[execute]] should update the output, such that the target is not 'dirty' any more.
     * @param execution
     * @param phase
     * @return
     */
    override def dirty(execution: Execution, phase: Phase) : Trilean = {
        phase match {
            case Phase.BUILD => Yes
            case _ => No
        }
    }

    /**
      * Build the "console" target by dumping records to stdout
      *
      * @param executor
      */
    override def build(executor:Execution) : Unit = {
        require(executor != null)

        val dfIn = dataset.read(executor)
        val dfOut = if (columns.nonEmpty)
            dfIn.select(columns.map(c => dfIn(c)):_*)
        else
            dfIn

        ConsoleUtils.showDataFrame(dfOut, limit, csv)
    }
}



class ConsoleTargetSpec extends TargetSpec {
    @JsonProperty(value="input", required=true) private var input:DatasetSpec = _
    @JsonProperty(value="limit", required=false) private var limit:String = "100"
    @JsonProperty(value="header", required=false) private var header:String = "true"
    @JsonProperty(value="csv", required=false) private var csv:String = "true"
    @JsonProperty(value="columns", required=false) private var columns:Seq[String] = Seq()

    override def instantiate(context: Context): Target = {
        ConsoleTarget(
            instanceProperties(context),
            input.instantiate(context),
            context.evaluate(limit).toInt,
            context.evaluate(header).toBoolean,
            context.evaluate(csv).toBoolean,
            columns.map(context.evaluate)
        )
    }
}
