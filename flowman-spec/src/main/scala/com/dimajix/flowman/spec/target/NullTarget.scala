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

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetInstance


case class NullTarget(
    instanceProperties: Target.Properties,
    partition: Map[String,String]
) extends BaseTarget {
    /**
      * Returns an instance representing this target with the context
      *
      * @return
      */
    override def instance: TargetInstance = {
        TargetInstance(
            namespace.map(_.name).getOrElse(""),
            project.map(_.name).getOrElse(""),
            name,
            partition
        )
    }
}


object NullTargetSpec {
    def apply(name:String, partition:Map[String,String]=Map())  : NullTargetSpec = {
        val spec = new NullTargetSpec
        spec.name = name
        spec.partition = partition
        spec
    }
}
class NullTargetSpec extends TargetSpec {
    @JsonProperty(value="partition", required=false) private var partition:Map[String,String] = Map()

    override def instantiate(context: Context): NullTarget = {
        NullTarget(
            instanceProperties(context),
            context.evaluate(partition)
        )
    }
}
