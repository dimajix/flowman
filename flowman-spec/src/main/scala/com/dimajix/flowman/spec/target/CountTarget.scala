/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.MappingUtils
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target


object CountTarget {
    def apply(context: Context, mapping:MappingOutputIdentifier) : CountTarget = {
        new CountTarget(
            Target.Properties(context),
            mapping
        )
    }
}
case class CountTarget(
    instanceProperties:Target.Properties,
    mapping:MappingOutputIdentifier
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
            case Phase.BUILD => MappingUtils.requires(context, mapping.mapping)
            case _ => Set()
        }
    }

    /**
     * Returns the state of the target, specifically of any artifacts produces. If this method return [[Yes]],
     * then an [[execute]] should update the output, such that the target is not 'dirty' any more.
     * @param executor
     * @param phase
     * @return
     */
    override def dirty(executor: Executor, phase: Phase) : Trilean = {
        phase match {
            case Phase.BUILD => Yes
            case _ => No
        }
    }

    /**
      * Build the "count" target by printing the number of records onto the console
      *
      * @param executor
      */
    override def build(executor:Executor) : Unit = {
        require(executor != null)

        val mapping = context.getMapping(this.mapping.mapping)
        val dfIn = executor.instantiate(mapping, this.mapping.output)
        val count = dfIn.count()
        System.out.println(s"Mapping '${this.mapping}' contains $count records")
    }
}


class CountTargetSpec extends TargetSpec {
    @JsonProperty(value = "mapping", required=true) private var mapping:String = _

    override def instantiate(context: Context): CountTarget = {
        CountTarget(
            Target.Properties(context),
            MappingOutputIdentifier.parse(context.evaluate(mapping))
        )
    }
}
