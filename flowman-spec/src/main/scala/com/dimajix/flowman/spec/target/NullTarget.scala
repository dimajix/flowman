/*
 * Copyright 2019-2022 Kaya Kupferschmidt
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
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetDigest


case class NullTarget(
    instanceProperties: Target.Properties,
    partition: Map[String,String]
) extends BaseTarget {
    /**
      * Returns an instance representing this target with the context
      *
      * @return
      */
    override def digest(phase:Phase): TargetDigest = {
        TargetDigest(
            namespace.map(_.name).getOrElse(""),
            project.map(_.name).getOrElse(""),
            name,
            phase,
            partition
        )
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
            case _ => No
        }
    }
}


object NullTargetSpec {
    def apply(name:String, partition:Map[String,String]=Map())  : NullTargetSpec = {
        val spec = new NullTargetSpec
        spec.name = name
        spec.partition = partition
        spec.kind = "null"
        spec
    }
}
class NullTargetSpec extends TargetSpec {
    @JsonProperty(value="partition", required=false) private var partition:Map[String,String] = Map()

    override def instantiate(context: Context, properties:Option[Target.Properties] = None): NullTarget = {
        NullTarget(
            instanceProperties(context, properties),
            context.evaluate(partition)
        )
    }
}
